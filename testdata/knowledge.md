# 文档处理流水线

Demo 19 使用独立 Package 完成文档加载、结构化解析和 Chunk。

## 能力契约

Parser 通过标准输出声明粒度和结构能力，Chunker 不根据文件扩展名猜测策略。

## 策略选择

- 结构化 Markdown 使用 Structure-aware Chunk。
- TXT、PDF、DOCX 和 XLSX 使用 Parent-child Chunk。

```go
result, err := workflow.Run(ctx, request)
```
