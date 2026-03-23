# AIAC2025_v3

WorldQuant BRAIN 量化因子挖掘平台

## 功能

- 数据探索：浏览数据集、字段、操作符
- Alpha 构建：手动或模板化生成表达式
- 批量回测：并发执行，支持断点续传
- AI 增强：智能分析，自动生成 Alpha
- 错误自愈：自动诊断修复失败的 Alpha

## 安装

```bash
pip install -r requirements.txt
```

## 配置

在 `config/` 目录创建凭证文件，详见 `使用说明.md`。

## 使用

```bash
# 交互式菜单
python main.py

# Alpha Factory Pipeline（推荐）
python main.py pipeline --region USA --datasets pv1
```

## 文档

- [使用说明.md](使用说明.md) - 完整使用指南
- [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md) - 项目详细说明

## 支持区域

USA, CHN, IND, EUR, ASI, GLB, JPN, KOR, TWN

## 许可证

MIT License