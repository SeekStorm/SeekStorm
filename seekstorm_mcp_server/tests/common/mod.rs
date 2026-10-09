use std::{path::Path, process::Stdio, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};

pub struct McpProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl McpProcess {
    pub async fn launch(index_path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_seekstorm_mcp_server"))
            .env("SEEKSTORM_INDEX_PATH", index_path)
            .env_remove("SEEKSTORM_INDEX_CONFIG")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn embedded MCP server");
        let stdin = child.stdin.take().expect("server stdin");
        let stdout = child.stdout.take().expect("server stdout");
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            next_id: 1,
        };

        let initialize = client
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "seekstorm-mcp-tests", "version": "0.1.0"}
                }),
            )
            .await;
        assert!(initialize["result"]["protocolVersion"].is_string());
        client.notify("notifications/initialized", json!({})).await;
        client
    }

    pub async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await;

        loop {
            let response = self.read_message().await;
            if response["id"] == id {
                return response;
            }
        }
    }

    pub async fn notify(&mut self, method: &str, params: Value) {
        self.write_message(json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .await;
    }

    #[allow(dead_code)]
    pub async fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        let response = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await;
        assert!(response.get("error").is_none(), "MCP error: {response}");
        response["result"].clone()
    }

    pub async fn shutdown(self) {
        let Self {
            mut child, stdin, ..
        } = self;
        drop(stdin);
        let status = timeout(Duration::from_secs(30), child.wait())
            .await
            .expect("server exits after stdio closes")
            .expect("wait for MCP server");
        assert!(status.success(), "MCP server exited with {status}");
    }

    async fn write_message(&mut self, value: Value) {
        let mut line = serde_json::to_vec(&value).expect("serialize JSON-RPC message");
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .expect("write JSON-RPC message");
        self.stdin.flush().await.expect("flush JSON-RPC message");
    }

    async fn read_message(&mut self) -> Value {
        let line = timeout(Duration::from_secs(30), self.stdout.next_line())
            .await
            .expect("MCP response timeout")
            .expect("read MCP response");
        let line = line.expect("MCP server closed stdout unexpectedly");
        serde_json::from_str(&line)
            .unwrap_or_else(|err| panic!("invalid JSON-RPC line from MCP server: {err}: {line:?}"))
    }
}

#[allow(dead_code)]
pub fn content_text(result: &Value) -> &str {
    result["content"][0]["text"]
        .as_str()
        .expect("text content in MCP tool result")
}
