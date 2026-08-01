# Go 快速开始

> jeeflow 引擎的 **Go 实现**——对齐 Java 参考实现的行为语义。本文面向 Go 开发者：安装、启动演示站、跑测试、生产部署。

## 环境要求

- Go 1.21+
- 零外部依赖（引擎核心纯 stdlib）；演示站用 GoFrame（`github.com/gogf/gf/v2`）

## 启动演示站（:8081）

```bash
# 推荐：编译后直接跑（比 go run 稳定，进程可单独 kill，避免旧进程占端口）
go build -o demo.exe ./cmd/demo
./demo.exe
# → http://localhost:8081
```

> 演示站从 `jeeflow-java` 的共享流程 JSON 加载 10 个示例流程（候选路径链兼容不同启动目录）。对接 jeeflow-ui（:5173）时右上角切到 `🔷 Go :8081`；接口规范见 [文档站 REST API 指南](https://jeeflow-doc.mldong.com/guides/03-api)。

## 快速验证

```bash
B=http://localhost:8081
curl -s -X POST $B/wf/processDefine/page -H "Content-Type: application/json" -d '{}'   # → {"code":0,"msg":"成功",...}
curl -s -X POST $B/wf/processDefine/startAndExecute -H "Content-Type: application/json" -d '{"processDefineId":9,"operator":"user1","amount":500}'
```

完整验证矩阵（同意/拒绝/退回发起人/highLight/approvalRecord）见文档站通用指南。

## 运行测试

```bash
go test ./...
```

## 生产部署

```bash
go build -o demo ./cmd/demo
./demo
```

生产接入：实现 `ProcessRepository` SPI（内存/DB 随意），映射 [SPEC §2](https://jeeflow-doc.mldong.com/spec/) 的 5 张表。

## 常见问题

| 症状 | 原因 | 处理 |
|------|------|------|
| 响应缺 `msg` 字段 / 行为是旧版 | 旧进程占着 8081（`go run` 遗留） | `netstat -ano \| grep :8081` → `taskkill //F //PID <pid>` → 重新 build 启动 |
| 浏览器报 CORS preflight 失败 | GoFrame `SetConfigWithMap Cors` 对未注册路由的 OPTIONS 预检不生效 | 用 `s.BindMiddlewareDefault` 全局中间件 + OPTIONS 短路（见 `cmd/demo/main.go`） |
