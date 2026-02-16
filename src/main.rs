use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
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
        /// Public TCP bind address where external clients connect (e.g. 0.0.0.0:2222).
        #[arg(long, env = "PROXY_PUBLIC_BIND", default_value = "0.0.0.0:2222")]
        public_bind: String,
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
}

#[derive(Clone)]
struct RelayState {
    token: Arc<String>,
    pending: Arc<Mutex<HashMap<u64, TcpStream>>>,
    control_writer: Arc<Mutex<Option<Arc<Mutex<OwnedWriteHalf>>>>>,
    next_session_id: Arc<AtomicU64>,
    open_timeout: Duration,
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
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Relay {
            public_bind,
            control_bind,
            data_bind,
            token,
            open_timeout_secs,
        } => {
            let state = RelayState {
                token: Arc::new(token),
                pending: Arc::new(Mutex::new(HashMap::new())),
                control_writer: Arc::new(Mutex::new(None)),
                next_session_id: Arc::new(AtomicU64::new(1)),
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
    };

    if let Err(err) = result {
        eprintln!("fatal error: {err}");
        std::process::exit(1);
    }
}

async fn run_relay(
    public_bind: String,
    control_bind: String,
    data_bind: String,
    state: RelayState,
) -> io::Result<()> {
    let public_listener = TcpListener::bind(&public_bind).await?;
    let control_listener = TcpListener::bind(&control_bind).await?;
    let data_listener = TcpListener::bind(&data_bind).await?;

    println!("relay listening: public={public_bind}, control={control_bind}, data={data_bind}");

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

    loop {
        let (client_stream, client_addr) = public_listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_public_client(state, client_stream, client_addr).await {
                eprintln!("public client handling error ({client_addr}): {err}");
            }
        });
    }
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
    {
        let mut guard = state.control_writer.lock().await;
        *guard = Some(writer_arc.clone());
    }

    println!("agent connected on control plane from {addr}");

    let mut clear_slot_on_exit = true;

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
                let removed = state.pending.lock().await.remove(&id);
                if removed.is_some() {
                    println!("session {id} failed by agent signal");
                }
            }
            Err(err) => {
                eprintln!("ignoring invalid control message from {addr}: {err}");
            }
        }
    }

    if clear_slot_on_exit {
        let mut guard = state.control_writer.lock().await;
        if let Some(current_writer) = guard.as_ref() {
            if Arc::ptr_eq(current_writer, &writer_arc) {
                *guard = None;
            } else {
                clear_slot_on_exit = false;
            }
        }
    }

    if clear_slot_on_exit {
        println!("agent disconnected from control plane: {addr}");
    }

    Ok(())
}

async fn handle_public_client(
    state: RelayState,
    client_stream: TcpStream,
    client_addr: SocketAddr,
) -> io::Result<()> {
    let session_id = state.next_session_id.fetch_add(1, Ordering::Relaxed);

    state.pending.lock().await.insert(session_id, client_stream);

    let writer = {
        let guard = state.control_writer.lock().await;
        guard.clone()
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

    println!("opened session {session_id} for public client {client_addr}");

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

    let _ = tokio::io::copy_bidirectional(&mut public_stream, &mut stream).await;
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
