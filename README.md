# eino-flow

基于 Eino 的通用 AI Workflow 编排层。仓库当前完成了第一个可运行闭环：文档索引工作流。

当前实现迁移自 `eino-lab` 的 demo19，源快照为 `31aba6b`。迁移过程中保留了已经验证的文档摄取、结构化解析、自动切分和模拟下游，同时抽取了仓库内部可复用的工作流运行层。

## 当前能力

| 阶段 | 状态 | 说明 |
|------|------|------|
| 文档摄取 | 真实 | 加载本地文件或受安全策略约束的 HTTP/HTTPS URL |
| 文档解析 | 真实 | Markdown 使用独立结构化 Parser，其他格式使用 ingestion 默认 Parser |
| 文档切分 | 真实 | 根据 Parser 输出能力选择 Structure-aware 或 Parent-child 策略 |
| Embedding | 模拟 | 不调用模型 |
| 索引持久化 | 模拟 | 不连接数据库 |
| 索引校验 | 模拟 | 不伪造校验成功 |
| 索引发布 | 模拟 | 不切换线上索引版本 |
| 结果构建 | 真实 | 返回 Parser、Chunk、关系、统计和各阶段状态 |

稳定工作流标识为 `rag_document_indexing@v4`，成功状态为 `chunked_with_simulated_downstream`。

## 当前架构

```mermaid
flowchart TB
    Entry[cmd/rag-index-dev<br/>本地装配与运行入口]

    subgraph Platform[公共工作流运行层]
        Runner[Runner<br/>Compile once / Run many]
        Governance[Descriptor / RunID<br/>Observer / OperationError]
        Runner --- Governance
    end

    subgraph RAG[RAG 索引工作流]
        Ingest[ingest_document<br/>真实]
        Chunk[chunk_document<br/>真实]
        Embed[embed_chunks<br/>模拟]
        Persist[persist_index<br/>模拟]
        Validate[validate_index<br/>模拟]
        Publish[publish_index<br/>模拟]
        Result[build_result<br/>真实]

        Ingest --> Chunk --> Embed --> Persist --> Validate --> Publish --> Result
    end

    subgraph Packages[独立文档处理模块]
        Ingestion[eino-document-ingestion]
        Parser[eino-document-parser-structured]
        Chunking[eino-document-chunking]
    end

    Entry --> Runner --> Ingest
    Ingest --> Ingestion
    Ingestion --> Parser
    Chunk --> Chunking

    classDef real fill:#e8f5e9,stroke:#2e7d32,color:#1b1b1b;
    classDef simulated fill:#fff8e1,stroke:#f57f17,stroke-dasharray: 5 5,color:#1b1b1b;
    class Ingest,Chunk,Result real;
    class Embed,Persist,Validate,Publish simulated;
```

公共层只管理工作流运行，不依赖 RAG 业务类型：

- `Descriptor`：稳定的工作流名称和定义版本。
- `RunID`：由调用入口提供的一次执行标识。
- `Runner`：启动时编译一次，并发复用已编译拓扑。
- `Observer`：把日志、Trace 或指标适配为 Eino Callback。
- `OperationError`：补充工作流、版本、RunID 和生命周期阶段，同时保留原始错误链。
- `RunOption`：只暴露受控的 Observer 和最大步数等运行能力。

## 目录结构

```text
.
├── cmd/rag-index-dev/             本地运行与 Eino Dev 入口
├── internal/platform/workflow/    公共工作流运行层
├── internal/rag/indexing/         文档索引工作流
├── testdata/knowledge.md           默认测试语料
├── docs/plans/                    后续演进路线
├── go.mod
└── go.sum
```

`internal/rag/indexing` 独立拥有自己的 Request、Result、Dependencies、状态和拓扑。公共运行层不知道文档、Chunk 或索引阶段是什么。

## 运行

仓库使用 Go `1.26.x`、Eino `v0.9.12`。默认示例不需要模型、数据库或 API Key。

运行默认 Markdown：

```bash
go run ./cmd/rag-index-dev
```

解析指定文件或 URL：

```bash
go run ./cmd/rag-index-dev /absolute/path/to/document.txt
go run ./cmd/rag-index-dev https://example.com/document.md
```

输出包含完整 Chunk 正文，适合本地开发和测试，不应直接作为生产日志格式。

## Eino Dev

```bash
EINO_DEV=true go run ./cmd/rag-index-dev
```

在 Eino Dev 中连接 `127.0.0.1:52538`，选择 `rag_document_indexing@v4`。该模式只用于本地调试，不应暴露到公网。

## 验证

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

测试覆盖 Markdown Structure-aware、TXT Parent-child、稳定 ID、语义路径、输入不可变性、错误链、超大原子块和公共 Runner 并发复用。

## 后续演进

真实索引下游的入库边界、表结构、写入校验和发布流程见 [RAG 索引入库设计 V1](docs/designs/rag-index-storage-v1.md)。其余后续方向维护在 [演进计划](docs/plans/2026-08-11-demo19-rag-migration.md)中。每个里程碑开始前先单独完成设计讨论和决策确认，再进入实现；当前不会提前实现真实索引下游或查询工作流。
