use clap::{Parser, Subcommand};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, watch};
use tokio::time::{Duration, sleep};

const MAX_LINE_LEN: usize = 1024;

#[derive(Parser, Debug)]
#[command(author, version, about = "Reverse TCP proxy (relay + agent)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Runs on the public server and accepts public clients + agent connections.
    Relay {
        /// Public bind host/IP where assigned ports are opened (e.g. 0.0.0.0).
        #[arg(long, env = "PROXY_PUBLIC_BIND", default_value = "0.0.0.0")]
        public_bind: String,
        /// Start of public port range assigned to agents.
        #[arg(long, env = "PROXY_PUBLIC_PORT_START", default_value_t = 2222)]
        public_port_start: u16,
        /// End of public port range assigned to agents.
        #[arg(long, env = "PROXY_PUBLIC_PORT_END", default_value_t = 2300)]
        public_port_end: u16,
        /// Control plane bind address for the home agent.
        #[arg(long, env = "PROXY_CONTROL_BIND", default_value = "0.0.0.0:7000")]
        control_bind: String,
        /// Data plane bind address for per-session streams from home agent.
        #[arg(long, env = "PROXY_DATA_BIND", default_value = "0.0.0.0:7001")]
        data_bind: String,
        /// Shared token between relay and agent.
        #[arg(long, env = "PROXY_TOKEN")]
        token: String,
        /// Timeout waiting for agent to attach a data channel for a public connection.
        #[arg(long, env = "PROXY_OPEN_TIMEOUT_SECS", default_value_t = 20)]
        open_timeout_secs: u64,
    },
    /// Runs at home, connects out to the relay, and forwards to local target.
    Agent {
        /// Relay host or IP (e.g. my-public-server.example.com).
        #[arg(long, env = "PROXY_RELAY_HOST")]
        relay_host: String,
        /// Relay control port.
        #[arg(long, env = "PROXY_CONTROL_PORT", default_value_t = 7000)]
        control_port: u16,
        /// Relay data port.
        #[arg(long, env = "PROXY_DATA_PORT", default_value_t = 7001)]
        data_port: u16,
        /// Local target to expose via relay (e.g. 127.0.0.1:22).
        #[arg(long, env = "PROXY_TARGET", default_value = "127.0.0.1:22")]
        target: String,
        /// Shared token between relay and agent.
        #[arg(long, env = "PROXY_TOKEN")]
        token: String,
        /// Backoff between reconnect attempts.
        #[arg(long, env = "PROXY_RECONNECT_SECS", default_value_t = 3)]
        reconnect_secs: u64,
    },
    /// Interactive terminal setup wizard for client or agent.
    Setup,
}

#[derive(Clone)]
struct RelayState {
    token: Arc<String>,
    pending: Arc<Mutex<HashMap<u64, PendingSession>>>,
    agents: Arc<Mutex<HashMap<u64, AgentHandle>>>,
    next_session_id: Arc<AtomicU64>,
    next_agent_id: Arc<AtomicU64>,
    port_pool: Arc<Mutex<PortPool>>,
    public_bind_host: Arc<String>,
    open_timeout: Duration,
}

struct PendingSession {
    agent_id: u64,
    stream: TcpStream,
}

#[derive(Clone)]
struct AgentHandle {
    writer: Arc<Mutex<OwnedWriteHalf>>,
    assigned_port: u16,
    shutdown_tx: watch::Sender<bool>,
}

struct PortPool {
    start: u16,
    end: u16,
    next: u16,
    in_use: HashSet<u16>,
}

impl PortPool {
    fn new(start: u16, end: u16) -> io::Result<Self> {
        if start > end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid port range: start {start} is greater than end {end}"),
            ));
        }
        Ok(Self {
            start,
            end,
            next: start,
            in_use: HashSet::new(),
        })
    }

    fn reserve_next(&mut self) -> io::Result<u16> {
        let total_ports = usize::from(self.end - self.start) + 1;
        for _ in 0..total_ports {
            let candidate = self.next;
            self.next = if self.next == self.end {
                self.start
            } else {
                self.next + 1
            };
            if self.in_use.insert(candidate) {
                return Ok(candidate);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("no free public ports in range {}-{}", self.start, self.end),
        ))
    }

    fn release(&mut self, port: u16) {
        self.in_use.remove(&port);
    }
}

#[derive(Clone)]
struct AgentConfig {
    relay_host: Arc<String>,
    control_port: u16,
    data_port: u16,
    target: Arc<String>,
    token: Arc<String>,
    reconnect_delay: Duration,
}

#[derive(Debug)]
enum RelayControlMsg {
    Fail(u64),
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Relay {
            public_bind,
            public_port_start,
            public_port_end,
            control_bind,
            data_bind,
            token,
            open_timeout_secs,
        } => {
            let port_pool = match PortPool::new(public_port_start, public_port_end) {
                Ok(pool) => pool,
                Err(err) => return Err(err),
            };
            let state = RelayState {
                token: Arc::new(token),
                pending: Arc::new(Mutex::new(HashMap::new())),
                agents: Arc::new(Mutex::new(HashMap::new())),
                next_session_id: Arc::new(AtomicU64::new(1)),
                next_agent_id: Arc::new(AtomicU64::new(1)),
                port_pool: Arc::new(Mutex::new(port_pool)),
                public_bind_host: Arc::new(public_bind.clone()),
                open_timeout: Duration::from_secs(open_timeout_secs),
            };
            run_relay(public_bind, control_bind, data_bind, state).await
        }
        Command::Agent {
            relay_host,
            control_port,
            data_port,
            target,
            token,
            reconnect_secs,
        } => {
            let config = AgentConfig {
                relay_host: Arc::new(relay_host),
                control_port,
                data_port,
                target: Arc::new(target),
                token: Arc::new(token),
                reconnect_delay: Duration::from_secs(reconnect_secs),
            };
            run_agent(config).await
        }
        Command::Setup => run_setup_wizard(),
    }
}

async fn run_relay(
    public_bind: String,
    control_bind: String,
    data_bind: String,
    state: RelayState,
) -> io::Result<()> {
    let control_listener = TcpListener::bind(&control_bind).await?;
    let data_listener = TcpListener::bind(&data_bind).await?;

    println!(
        "relay listening: public-host={public_bind}, control={control_bind}, data={data_bind}"
    );

    {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_control_listener(control_listener, state).await {
                eprintln!("control listener stopped: {err}");
            }
        });
    }

    {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_data_listener(data_listener, state).await {
                eprintln!("data listener stopped: {err}");
            }
        });
    }

    std::future::pending::<()>().await;
    Ok(())
}

async fn run_control_listener(listener: TcpListener, state: RelayState) -> io::Result<()> {
    loop {
        let (stream, addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_control_connection(stream, addr, state).await {
                eprintln!("control connection error ({addr}): {err}");
            }
        });
    }
}

async fn run_data_listener(listener: TcpListener, state: RelayState) -> io::Result<()> {
    loop {
        let (stream, addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_data_connection(stream, addr, state).await {
                eprintln!("data connection error ({addr}): {err}");
            }
        });
    }
}

async fn handle_control_connection(
    stream: TcpStream,
    addr: SocketAddr,
    state: RelayState,
) -> io::Result<()> {
    let (mut reader, writer) = stream.into_split();
    let auth_line = read_line(&mut reader, MAX_LINE_LEN).await?;
    let auth_token = parse_auth_line(&auth_line)?;

    if auth_token != state.token.as_str() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid token on control connection",
        ));
    }

    let writer_arc = Arc::new(Mutex::new(writer));
    let agent_id = state.next_agent_id.fetch_add(1, Ordering::Relaxed);
    let (assigned_port, public_listener) = reserve_and_bind_public_listener(&state).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    {
        let mut agents = state.agents.lock().await;
        agents.insert(
            agent_id,
            AgentHandle {
                writer: writer_arc.clone(),
                assigned_port,
                shutdown_tx: shutdown_tx.clone(),
            },
        );
    }

    let assigned_msg = format!("ASSIGNED {assigned_port}\n");
    {
        let mut guard = writer_arc.lock().await;
        guard.write_all(assigned_msg.as_bytes()).await?;
    }

    println!("agent {agent_id} connected from {addr}; assigned public port {assigned_port}");

    {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) =
                run_agent_public_listener(public_listener, state, agent_id, shutdown_rx).await
            {
                eprintln!("agent {agent_id} public listener stopped: {err}");
            }
        });
    }

    loop {
        let line = match read_line(&mut reader, MAX_LINE_LEN).await {
            Ok(line) => line,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                println!("control connection closed from {addr}");
                break;
            }
            Err(err) => return Err(err),
        };

        match parse_relay_control_msg(&line) {
            Ok(RelayControlMsg::Fail(id)) => {
                let removed = remove_pending_session_for_agent(&state, id, agent_id).await;
                if removed.is_some() {
                    println!("session {id} failed by agent {agent_id} signal");
                }
            }
            Err(err) => {
                eprintln!("ignoring invalid control message from {addr}: {err}");
            }
        }
    }

    let removed_agent = {
        let mut agents = state.agents.lock().await;
        agents.remove(&agent_id)
    };

    if let Some(agent) = removed_agent {
        let _ = agent.shutdown_tx.send(true);
        state.port_pool.lock().await.release(agent.assigned_port);
        drop_pending_for_agent(&state, agent_id).await;
        println!(
            "agent {agent_id} disconnected from control plane: {addr} (released public port {})",
            agent.assigned_port
        );
    }

    Ok(())
}

async fn handle_public_client(
    state: RelayState,
    client_stream: TcpStream,
    client_addr: SocketAddr,
    agent_id: u64,
) -> io::Result<()> {
    let session_id = state.next_session_id.fetch_add(1, Ordering::Relaxed);

    state.pending.lock().await.insert(
        session_id,
        PendingSession {
            agent_id,
            stream: client_stream,
        },
    );

    let writer = {
        let guard = state.agents.lock().await;
        guard.get(&agent_id).map(|agent| agent.writer.clone())
    };

    let Some(writer) = writer else {
        let _ = state.pending.lock().await.remove(&session_id);
        return Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "no active agent control connection",
        ));
    };

    let open_msg = format!("OPEN {session_id}\n");
    {
        let mut writer_guard = writer.lock().await;
        if let Err(err) = writer_guard.write_all(open_msg.as_bytes()).await {
            let _ = state.pending.lock().await.remove(&session_id);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("failed to send OPEN message: {err}"),
            ));
        }
    }

    println!("opened session {session_id} for public client {client_addr} via agent {agent_id}");

    let pending = state.pending.clone();
    let timeout = state.open_timeout;
    tokio::spawn(async move {
        sleep(timeout).await;
        if pending.lock().await.remove(&session_id).is_some() {
            eprintln!("session {session_id} timed out waiting for agent data channel");
        }
    });

    Ok(())
}

async fn handle_data_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    state: RelayState,
) -> io::Result<()> {
    let line = read_line(&mut stream, MAX_LINE_LEN).await?;
    let (session_id, token) = parse_data_handshake(&line)?;

    if token != state.token.as_str() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid token on data connection",
        ));
    }

    let Some(mut public_stream) = state.pending.lock().await.remove(&session_id) else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("unknown or expired session id: {session_id}"),
        ));
    };

    let _ = tokio::io::copy_bidirectional(&mut public_stream.stream, &mut stream).await;
    println!("session {session_id} closed (data peer: {addr})");

    Ok(())
}

async fn run_agent(config: AgentConfig) -> io::Result<()> {
    let control_addr = format!("{}:{}", config.relay_host, config.control_port);
    let data_addr = format!("{}:{}", config.relay_host, config.data_port);

    loop {
        match TcpStream::connect(&control_addr).await {
            Ok(control_stream) => {
                println!("connected to relay control plane at {control_addr}");
                if let Err(err) =
                    handle_agent_control_session(control_stream, &data_addr, &config).await
                {
                    eprintln!("control session ended with error: {err}");
                }
            }
            Err(err) => {
                eprintln!("failed to connect control plane {control_addr}: {err}");
            }
        }

        sleep(config.reconnect_delay).await;
    }
}

async fn handle_agent_control_session(
    control_stream: TcpStream,
    data_addr: &str,
    config: &AgentConfig,
) -> io::Result<()> {
    let (mut reader, mut writer) = control_stream.into_split();
    let auth_msg = format!("AUTH {}\n", config.token.as_str());
    writer.write_all(auth_msg.as_bytes()).await?;

    let writer = Arc::new(Mutex::new(writer));
    let assigned_line = read_line(&mut reader, MAX_LINE_LEN).await?;
    let assigned_port = parse_assigned_line(&assigned_line)?;
    println!("assigned public relay port {assigned_port}");

    loop {
        let line = read_line(&mut reader, MAX_LINE_LEN).await?;
        let session_id = parse_open_line(&line)?;

        let cfg = config.clone();
        let data_addr = data_addr.to_string();
        let writer = writer.clone();

        tokio::spawn(async move {
            if let Err(err) = handle_agent_open(session_id, &data_addr, cfg, writer).await {
                eprintln!("session {session_id} handling error: {err}");
            }
        });
    }
}

async fn handle_agent_open(
    session_id: u64,
    data_addr: &str,
    config: AgentConfig,
    control_writer: Arc<Mutex<OwnedWriteHalf>>,
) -> io::Result<()> {
    let mut local_stream = match TcpStream::connect(config.target.as_str()).await {
        Ok(stream) => stream,
        Err(err) => {
            send_fail(&control_writer, session_id).await;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("failed to connect local target {}: {err}", config.target),
            ));
        }
    };

    let mut relay_data_stream = match TcpStream::connect(data_addr).await {
        Ok(stream) => stream,
        Err(err) => {
            send_fail(&control_writer, session_id).await;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("failed to connect relay data plane {data_addr}: {err}"),
            ));
        }
    };

    let handshake = format!("ID {session_id} {}\n", config.token);
    relay_data_stream.write_all(handshake.as_bytes()).await?;

    let _ = tokio::io::copy_bidirectional(&mut local_stream, &mut relay_data_stream).await;
    Ok(())
}

async fn send_fail(writer: &Arc<Mutex<OwnedWriteHalf>>, session_id: u64) {
    let msg = format!("FAIL {session_id}\n");
    let mut guard = writer.lock().await;
    let _ = guard.write_all(msg.as_bytes()).await;
}

async fn read_line<R>(reader: &mut R, max_len: usize) -> io::Result<String>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(128);

    loop {
        let mut byte = [0_u8; 1];
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            if buf.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed",
                ));
            }
            break;
        }

        if byte[0] == b'\n' {
            break;
        }

        if buf.len() >= max_len {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "line too long"));
        }

        if byte[0] != b'\r' {
            buf.push(byte[0]);
        }
    }

    String::from_utf8(buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))
}

fn parse_auth_line(line: &str) -> io::Result<&str> {
    let (cmd, rest) = split_two(line)?;
    if cmd != "AUTH" || rest.trim().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid auth line: {line}"),
        ));
    }
    Ok(rest.trim())
}

fn parse_relay_control_msg(line: &str) -> io::Result<RelayControlMsg> {
    let (cmd, rest) = split_two(line)?;
    match cmd {
        "FAIL" => {
            let id = parse_u64(rest.trim(), "FAIL id")?;
            Ok(RelayControlMsg::Fail(id))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown control message: {line}"),
        )),
    }
}

fn parse_data_handshake(line: &str) -> io::Result<(u64, &str)> {
    let mut parts = line.splitn(3, ' ');
    let cmd = parts.next().unwrap_or_default();
    let id = parts.next().unwrap_or_default();
    let token = parts.next().unwrap_or_default().trim();

    if cmd != "ID" || token.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid data handshake: {line}"),
        ));
    }

    Ok((parse_u64(id, "session id")?, token))
}

fn parse_open_line(line: &str) -> io::Result<u64> {
    let (cmd, rest) = split_two(line)?;
    if cmd != "OPEN" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid OPEN line: {line}"),
        ));
    }
    parse_u64(rest.trim(), "session id")
}

fn parse_assigned_line(line: &str) -> io::Result<u16> {
    let (cmd, rest) = split_two(line)?;
    if cmd != "ASSIGNED" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid ASSIGNED line: {line}"),
        ));
    }
    parse_u16(rest.trim(), "assigned port")
}

fn split_two(line: &str) -> io::Result<(&str, &str)> {
    let mut parts = line.splitn(2, ' ');
    let first = parts.next().unwrap_or_default();
    let second = parts.next().unwrap_or_default();

    if first.is_empty() || second.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid command line: {line}"),
        ));
    }

    Ok((first, second))
}

fn parse_u64(value: &str, label: &str) -> io::Result<u64> {
    value.parse::<u64>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse {label} as u64 ('{value}'): {err}"),
        )
    })
}

fn parse_u16(value: &str, label: &str) -> io::Result<u16> {
    value.parse::<u16>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse {label} as u16 ('{value}'): {err}"),
        )
    })
}

fn run_setup_wizard() -> io::Result<()> {
    println!("TCP Proxy setup wizard");
    println!();
    println!("Select role:");
    println!("1) Agent (home machine exposing a local service)");
    println!("2) Client (machine connecting to an assigned public port)");
    println!();

    let selection = prompt_u8("Enter choice", Some(1))?;
    match selection {
        1 => run_agent_setup_wizard(),
        2 => run_client_setup_wizard(),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported setup role selection",
        )),
    }
}

fn run_agent_setup_wizard() -> io::Result<()> {
    println!();
    println!("Agent setup");
    println!("-----------");

    let relay_host = prompt_string("Relay host / IP", None)?;
    let control_port = prompt_u16("Control port", Some(7000))?;
    let data_port = prompt_u16("Data port", Some(7001))?;
    let target = prompt_string("Local target (host:port)", Some("127.0.0.1:22"))?;
    let token = prompt_string("Shared token", None)?;
    let reconnect_secs = prompt_u64("Reconnect seconds", Some(3))?;

    println!();
    println!("Run this command:");
    println!(
        "cargo run -- agent --relay-host {} --control-port {} --data-port {} --target {} --token {} --reconnect-secs {}",
        shell_quote(&relay_host),
        control_port,
        data_port,
        shell_quote(&target),
        shell_quote(&token),
        reconnect_secs
    );

    println!();
    println!("Or with environment variables:");
    println!("export PROXY_RELAY_HOST={}", shell_quote(&relay_host));
    println!("export PROXY_CONTROL_PORT={control_port}");
    println!("export PROXY_DATA_PORT={data_port}");
    println!("export PROXY_TARGET={}", shell_quote(&target));
    println!("export PROXY_TOKEN={}", shell_quote(&token));
    println!("export PROXY_RECONNECT_SECS={reconnect_secs}");
    println!("cargo run -- agent");

    Ok(())
}

fn run_client_setup_wizard() -> io::Result<()> {
    println!();
    println!("Client setup");
    println!("------------");
    println!("Use the assigned port printed by the agent when it connects.");

    let public_host = prompt_string("Public relay host / IP", None)?;
    let assigned_port = prompt_u16("Assigned public port", None)?;
    let username = prompt_string("SSH username", Some("root"))?;

    println!();
    println!("Connect with:");
    println!(
        "ssh -p {} {}@{}",
        assigned_port,
        shell_quote(&username),
        shell_quote(&public_host)
    );

    Ok(())
}

fn prompt_string(prompt: &str, default: Option<&str>) -> io::Result<String> {
    loop {
        match default {
            Some(value) => print!("{prompt} [{value}]: "),
            None => print!("{prompt}: "),
        }
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();

        if trimmed.is_empty() {
            if let Some(value) = default {
                return Ok(value.to_string());
            }
            eprintln!("A value is required.");
            continue;
        }

        return Ok(trimmed.to_string());
    }
}

fn prompt_u16(prompt: &str, default: Option<u16>) -> io::Result<u16> {
    loop {
        let value = prompt_string(prompt, default.map(|v| v.to_string()).as_deref())?;
        match value.parse::<u16>() {
            Ok(parsed) => return Ok(parsed),
            Err(_) => eprintln!("Please enter a valid number in range 0-65535."),
        }
    }
}

fn prompt_u64(prompt: &str, default: Option<u64>) -> io::Result<u64> {
    loop {
        let value = prompt_string(prompt, default.map(|v| v.to_string()).as_deref())?;
        match value.parse::<u64>() {
            Ok(parsed) => return Ok(parsed),
            Err(_) => eprintln!("Please enter a valid positive integer."),
        }
    }
}

fn prompt_u8(prompt: &str, default: Option<u8>) -> io::Result<u8> {
    loop {
        let value = prompt_string(prompt, default.map(|v| v.to_string()).as_deref())?;
        match value.parse::<u8>() {
            Ok(parsed) => return Ok(parsed),
            Err(_) => eprintln!("Please enter a valid choice."),
        }
    }
}

fn shell_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\\''");
    format!("'{escaped}'")
}

async fn reserve_and_bind_public_listener(state: &RelayState) -> io::Result<(u16, TcpListener)> {
    loop {
        let candidate_port = {
            let mut pool = state.port_pool.lock().await;
            pool.reserve_next()?
        };

        let bind_addr = format!("{}:{}", state.public_bind_host, candidate_port);
        match TcpListener::bind(&bind_addr).await {
            Ok(listener) => return Ok((candidate_port, listener)),
            Err(err) => {
                state.port_pool.lock().await.release(candidate_port);
                eprintln!("failed to bind assigned public port {candidate_port}: {err}");
            }
        }
    }
}

async fn run_agent_public_listener(
    listener: TcpListener,
    state: RelayState,
    agent_id: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (client_stream, client_addr) = accepted?;
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_public_client(state, client_stream, client_addr, agent_id).await {
                        eprintln!("public client handling error ({client_addr}): {err}");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn remove_pending_session_for_agent(
    state: &RelayState,
    session_id: u64,
    agent_id: u64,
) -> Option<PendingSession> {
    let mut pending = state.pending.lock().await;
    if pending
        .get(&session_id)
        .map(|session| session.agent_id == agent_id)
        .unwrap_or(false)
    {
        return pending.remove(&session_id);
    }
    None
}

async fn drop_pending_for_agent(state: &RelayState, agent_id: u64) {
    let mut pending = state.pending.lock().await;
    pending.retain(|_, session| session.agent_id != agent_id);
}
