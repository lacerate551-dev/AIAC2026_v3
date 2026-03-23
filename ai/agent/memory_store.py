# -*- coding: utf-8 -*-
"""
MemoryStore — 研究记忆持久化
存储在 memory/ 目录下：
- research_history.json — 每次研究的完整记录
- knowledge_base.json — 提炼的经验知识
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from config.settings import MEMORY_DIR
from config.agent_config import MEMORY_CONFIG

logger = logging.getLogger(__name__)


class MemoryStore:
    """研究记忆持久化管理"""

    def __init__(self):
        self.history_file = MEMORY_DIR / "research_history.json"
        self.knowledge_file = MEMORY_DIR / "knowledge_base.json"
        self._ensure_files()

    def _ensure_files(self):
        if not self.history_file.exists():
            self.history_file.write_text("[]", encoding="utf-8")
        if not self.knowledge_file.exists():
            self.knowledge_file.write_text("[]", encoding="utf-8")

    # ==================== 研究历史 ====================

    def save_research(self, record: Dict[str, Any]):
        """保存一条研究记录"""
        history = self._load_json(self.history_file)
        record["saved_at"] = datetime.now().isoformat()
        history.append(record)
        # FIFO 淘汰
        max_records = MEMORY_CONFIG["max_research_records"]
        if len(history) > max_records:
            history = history[-max_records:]
        self._save_json(self.history_file, history)
        logger.info(f"研究记录已保存，当前共 {len(history)} 条")

    def query_research(self, region: str = "", dataset_ids: List[str] = None) -> List[Dict]:
        """
        检索相关研究历史
        精确匹配：相同 region + datasets
        模糊匹配：region 相同，datasets 有交集
        """
        history = self._load_json(self.history_file)
        if not region and not dataset_ids:
            return history[-10:]  # 返回最近 10 条

        exact, fuzzy = [], []
        ds_set = set(dataset_ids or [])

        for rec in history:
            rec_region = rec.get("region", "")
            rec_ds = set(rec.get("dataset_ids", []))

            if rec_region == region and rec_ds == ds_set:
                exact.append(rec)
            elif rec_region == region and ds_set & rec_ds:
                fuzzy.append(rec)

        # 精确优先，模糊补充
        results = exact[-5:] + fuzzy[-5:]
        return results

    # ==================== 知识库 ====================

    def save_knowledge(self, entry: Dict[str, Any]):
        """保存一条知识条目"""
        kb = self._load_json(self.knowledge_file)
        entry["saved_at"] = datetime.now().isoformat()
        kb.append(entry)
        max_entries = MEMORY_CONFIG["max_knowledge_entries"]
        if len(kb) > max_entries:
            kb = kb[-max_entries:]
        self._save_json(self.knowledge_file, kb)

    def query_knowledge(self, region: str = "", keywords: List[str] = None) -> List[Dict]:
        """检索相关知识"""
        kb = self._load_json(self.knowledge_file)
        if not region and not keywords:
            return kb[-10:]

        results = []
        for entry in kb:
            score = 0
            if region and entry.get("region") == region:
                score += 2
            if keywords:
                entry_text = json.dumps(entry, ensure_ascii=False).lower()
                for kw in keywords:
                    if kw.lower() in entry_text:
                        score += 1
            if score > 0:
                results.append((score, entry))

        results.sort(key=lambda x: x[0], reverse=True)
        return [r[1] for r in results[:10]]

    def get_context_for_planning(self, region: str, dataset_ids: List[str] = None) -> str:
        """为 Planner 生成记忆上下文摘要"""
        research = self.query_research(region, dataset_ids)
        knowledge = self.query_knowledge(region)

        lines = []
        if research:
            lines.append("=== 相关研究历史 ===")
            for rec in research[-3:]:
                goal = rec.get("goal", "N/A")
                best_sharpe = rec.get("best_sharpe", "N/A")
                findings = rec.get("key_findings", "")
                lines.append(f"- 目标: {goal} | 最佳Sharpe: {best_sharpe}")
                if findings:
                    lines.append(f"  发现: {findings}")

        if knowledge:
            lines.append("\n=== 经验知识 ===")
            for entry in knowledge[-5:]:
                category = entry.get("category", "general")
                content = entry.get("content", "")
                lines.append(f"- [{category}] {content}")

        return "\n".join(lines) if lines else "暂无相关历史经验"

    # ==================== 内部工具 ====================

    def _load_json(self, filepath: Path) -> List:
        try:
            return json.loads(filepath.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _save_json(self, filepath: Path, data: List):
        filepath.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
