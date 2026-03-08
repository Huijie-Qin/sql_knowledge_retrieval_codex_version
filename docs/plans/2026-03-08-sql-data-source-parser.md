# SQL数据源解析工程 实现方案

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建一个本地Python工程，实现自动遍历案例目录下的.sql/.md文件，通过LLM解析生成标准化的数据源知识索引文件，支持增量更新和智能合并，避免上下文窗口爆炸问题。

**Architecture:**
1. 采用**端到端LLM解析**架构，直接将完整源文件内容+结构化Prompt输入LLM，替代原有的"SQL规则提取+LLM理解"两步法，提升解析质量
2. 采用**分块合并策略**解决更新时的上下文爆炸问题：将数据源文件按模块拆分，只对变更模块进行增量合并，而非全量传入
3. 实现**原子化文件处理**：单个文件处理完成立即落地结果和进度，支持断点续传
4. 内置**质量校验层**：自动验证解析结果的格式完整性和内容合理性

**Tech Stack:**
- Python 3.10+
- OpenAI SDK / 通义千问 SDK (支持自定义LLM接口)
- python-dotenv (配置管理)
- pydantic (数据结构校验)
- Jinja2 (Prompt模板管理)
- pytest (单元测试)

---

## 阶段1: 工程基础框架搭建

### Task 1: 项目目录结构初始化

**Files:**
- Create: `.gitignore`
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `src/__init__.py`
- Create: `config/__init__.py`
- Create: `config/settings.py`
- Create: `prompts/`
- Create: `tests/__init__.py`

**Step 1: 编写项目依赖文件**
```txt
# requirements.txt
openai>=1.0.0
python-dotenv>=1.0.0
pydantic>=2.0.0
jinja2>=3.1.0
pytest>=7.0.0
pyyaml>=6.0
markdown-it-py>=3.0.0
```

**Step 2: 编写环境变量示例文件**
```env
# .env.example
LLM_API_KEY=your_api_key_here
LLM_BASE_URL=https://api.openai.com/v1
LLM_MODEL=gpt-4o-mini
SOURCE_DIR=./案例
OUTPUT_DIR=./数据源
MAX_CONTEXT_TOKENS=128000
```

**Step 3: 编写.gitignore文件**
```gitignore
# Python
__pycache__/
*.pyc
*.pyo
*.pyd
.env
.vscode/
.idea/
*.log
数据源/
!数据源/.gitkeep
```

**Step 4: 安装依赖**
Run: `pip install -r requirements.txt`
Expected: All dependencies installed successfully

**Step 5: Commit**
```bash
git add .gitignore requirements.txt .env.example src/__init__.py config/__init__.py config/settings.py prompts/ tests/__init__.py
git commit -m "feat: initialize project structure and dependencies"
```

---

### Task 2: 配置管理模块实现

**Files:**
- Modify: `config/settings.py`
- Test: `tests/test_settings.py`

**Step 1: 编写配置加载测试**
```python
# tests/test_settings.py
from config.settings import Settings

def test_settings_load():
    settings = Settings()
    assert settings.llm_api_key is not None
    assert settings.source_dir == "./案例"
    assert settings.output_dir == "./数据源"
    assert settings.max_context_tokens > 0
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_settings.py -v`
Expected: FAIL with "Settings not defined"

**Step 3: 实现配置管理模块**
```python
# config/settings.py
from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    llm_api_key: str
    llm_base_url: str = "https://api.openai.com/v1"
    llm_model: str = "gpt-4o-mini"
    source_dir: Path = Path("./案例")
    output_dir: Path = Path("./数据源")
    max_context_tokens: int = 128000

    class Config:
        env_file = ".env"

settings = Settings()
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_settings.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add config/settings.py tests/test_settings.py
git commit -m "feat: implement configuration management module"
```

---

### Task 3: LLM客户端封装实现

**Files:**
- Create: `src/llm_client.py`
- Test: `tests/test_llm_client.py`

**Step 1: 编写LLM客户端测试**
```python
# tests/test_llm_client.py
from src.llm_client import LLMClient

def test_llm_client_call():
    client = LLMClient()
    response = client.chat("你好，请返回'OK'")
    assert "OK" in response
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_llm_client.py -v`
Expected: FAIL with "LLMClient not defined"

**Step 3: 实现LLM客户端封装**
```python
# src/llm_client.py
from openai import OpenAI
from config.settings import settings
from typing import Optional

class LLMClient:
    def __init__(self):
        self.client = OpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url
        )
        self.model = settings.llm_model

    def chat(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0.1,
            max_tokens=4096
        )

        return response.choices[0].message.content.strip()
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_llm_client.py -v`
Expected: PASS (需要配置正确的API密钥)

**Step 5: Commit**
```bash
git add src/llm_client.py tests/test_llm_client.py
git commit -m "feat: implement LLM client wrapper"
```

---

## 阶段2: 核心解析逻辑实现

### Task 4: Prompt模板设计与管理

**Files:**
- Create: `prompts/parse_md.j2`
- Create: `prompts/parse_sql.j2`
- Create: `prompts/merge_data_source.j2`
- Create: `src/prompt_manager.py`
- Test: `tests/test_prompt_manager.py`

**Step 1: 编写Prompt加载测试**
```python
# tests/test_prompt_manager.py
from src.prompt_manager import PromptManager

def test_prompt_load():
    manager = PromptManager()
    md_prompt = manager.get_prompt("parse_md", content="test content")
    sql_prompt = manager.get_prompt("parse_sql", content="test sql")
    merge_prompt = manager.get_prompt("merge_data_source", old="old", new="new")
    assert len(md_prompt) > 0
    assert len(sql_prompt) > 0
    assert len(merge_prompt) > 0
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_prompt_manager.py -v`
Expected: FAIL with "PromptManager not defined"

**Step 3: 实现Prompt管理器**
```python
# src/prompt_manager.py
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, Any

class PromptManager:
    def __init__(self, template_dir: str = "prompts"):
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def get_prompt(self, template_name: str, **kwargs: Dict[str, Any]) -> str:
        template = self.env.get_template(f"{template_name}.j2")
        return template.render(**kwargs)
```

**Step 4: 编写Markdown解析Prompt模板**
```jinja
{# prompts/parse_md.j2 #}
你是专业的SQL数据源分析师，请解析以下Markdown案例文件内容，提取其中的数据源信息。

文件内容：
{{ content }}

请严格按照以下JSON格式返回结果，不要添加任何额外说明：
{
  "business_domain": "识别所属业务域（广告/应用/音乐/公共/全域搜索/电商/其他）",
  "data_sources": [
    {
      "table_name": "完整表名",
      "description": "数据源描述",
      "fields": [
        {
          "name": "字段名",
          "description": "字段描述",
          "usage": "用途说明"
        }
      ],
      "sql_examples": [
        {
          "name": "示例名称",
          "sql": "SQL代码"
        }
      ],
      "usage_instructions": "使用说明",
      "notes": "注意事项",
      "data_quality": {
        "daily_records": "日记录数",
        "daily_users": "日覆盖用户数",
        "coverage": "数据覆盖情况",
        "timeliness": "上报及时性"
      },
      "related_cases": [
        {
          "name": "案例名称",
          "type": "案例类型",
          "scenario": "使用场景"
        }
      ]
    }
  ]
}
```

**Step 5: 编写SQL解析Prompt模板**
```jinja
{# prompts/parse_sql.j2 #}
你是专业的SQL数据源分析师，请解析以下SQL文件内容，提取其中的数据源信息。

文件内容：
{{ content }}
文件名: {{ filename }}

请严格按照以下JSON格式返回结果，不要添加任何额外说明：
{
  "business_domain": "从文件名和内容推断业务域（广告/应用/音乐/公共/全域搜索/电商/其他）",
  "data_sources": [
    {
      "table_name": "完整表名",
      "description": "数据源描述",
      "fields": [
        {
          "name": "字段名",
          "description": "字段描述",
          "usage": "用途说明"
        }
      ],
      "sql_examples": [
        {
          "name": "示例名称",
          "sql": "SQL代码"
        }
      ],
      "usage_instructions": "使用说明",
      "notes": "注意事项",
      "data_quality": {
        "daily_records": "",
        "daily_users": "",
        "coverage": "",
        "timeliness": ""
      },
      "related_cases": [
        {
          "name": "{{ filename }}",
          "type": "SQL案例",
          "scenario": "从SQL内容推断使用场景"
        }
      ]
    }
  ]
}
```

**Step 6: 编写数据源合并Prompt模板**
```jinja
{# prompts/merge_data_source.j2 #}
你是专业的数据合并专家，请合并两个数据源信息，保留所有有价值的信息，避免重复。

现有数据源内容：
{{ old_content }}

新提取的数据源内容：
{{ new_content }}

合并规则：
1. 以现有数据源为基础，补充新信息
2. 相同字段如果有更详细的描述，使用更详细的版本
3. 新增SQL示例和关联案例如果不存在则添加
4. 注意去重，相同内容不要重复添加
5. 如果有冲突的信息，优先保留更准确和详细的版本

请严格按照数据源文件的Markdown格式返回合并后的完整内容。
```

**Step 7: 运行测试验证通过**
Run: `pytest tests/test_prompt_manager.py -v`
Expected: PASS

**Step 8: Commit**
```bash
git add prompts/parse_md.j2 prompts/parse_sql.j2 prompts/merge_data_source.j2 src/prompt_manager.py tests/test_prompt_manager.py
git commit -m "feat: implement prompt manager and templates"
```

---

### Task 5: 文件解析器实现

**Files:**
- Create: `src/parser.py`
- Test: `tests/test_parser.py`

**Step 1: 编写解析器测试**
```python
# tests/test_parser.py
from src.parser import FileParser
from pathlib import Path

def test_parse_md_file():
    parser = FileParser()
    test_content = """
    # 电商分析案例
    用户行为表dwd_user_behavior_d包含用户的浏览、点击、购买等行为数据。
    字段包括did(设备id)，event_type(事件类型)，event_time(事件时间)。
    """
    result = parser.parse_md(test_content)
    assert "data_sources" in result
    assert len(result["data_sources"]) > 0

def test_parse_sql_file():
    parser = FileParser()
    test_content = """
    -- 电商用户购买分析
    SELECT did, count(*) as buy_cnt
    FROM dwd.dwd_user_behavior_d
    WHERE event_type = 'buy'
    GROUP BY did
    """
    result = parser.parse_sql(test_content, "analysis_ecommerce_buy.sql")
    assert "data_sources" in result
    assert len(result["data_sources"]) > 0
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_parser.py -v`
Expected: FAIL with "FileParser not defined"

**Step 3: 实现文件解析器**
```python
# src/parser.py
import json
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from typing import Dict, Any

class FileParser:
    def __init__(self):
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()
        self.system_prompt = "你是专业的SQL数据源分析师，严格按照要求返回JSON格式结果，不要添加任何额外说明。"

    def parse_md(self, content: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_md", content=content)
        response = self.llm_client.chat(prompt, self.system_prompt)
        # 清理可能的markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:-3].strip()
        return json.loads(response)

    def parse_sql(self, content: str, filename: str) -> Dict[str, Any]:
        prompt = self.prompt_manager.get_prompt("parse_sql", content=content, filename=filename)
        response = self.llm_client.chat(prompt, self.system_prompt)
        # 清理可能的markdown标记
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:-3].strip()
        return json.loads(response)
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_parser.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add src/parser.py tests/test_parser.py
git commit -m "feat: implement file parser with LLM"
```

---

## 阶段3: 数据源管理与合并逻辑

### Task 6: 数据源文件管理器实现

**Files:**
- Create: `src/data_source_manager.py`
- Test: `tests/test_data_source_manager.py`

**Step 1: 编写数据源管理器测试**
```python
# tests/test_data_source_manager.py
from src.data_source_manager import DataSourceManager
from pathlib import Path

def test_create_data_source():
    manager = DataSourceManager()
    source_data = {
        "table_name": "test.dwd_test_table",
        "description": "测试表",
        "business_domain": "测试",
        "fields": [{"name": "id", "description": "主键", "usage": "唯一标识"}]
    }
    file_path = manager.create_data_source(source_data, "测试")
    assert file_path.exists()
    assert file_path.read_text().find("test.dwd_test_table") > -1

def test_merge_data_source():
    manager = DataSourceManager()
    old_content = """
    # test.dwd_test_table
    ## 1.数据源基本信息
    ### 1.1.数据源名称
    测试表
    ### 1.2.数据源描述
    测试表描述
    """
    new_data = {
        "table_name": "test.dwd_test_table",
        "description": "更新后的测试表描述",
        "fields": [{"name": "name", "description": "名称", "usage": "用户名称"}]
    }
    merged_content, update_points = manager.merge_data_source(old_content, new_data)
    assert "更新后的测试表描述" in merged_content
    assert "name" in merged_content
    assert len(update_points) > 0
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_data_source_manager.py -v`
Expected: FAIL with "DataSourceManager not defined"

**Step 3: 实现数据源管理器**
```python
# src/data_source_manager.py
from pathlib import Path
from typing import Dict, Any, Tuple, List
from src.llm_client import LLMClient
from src.prompt_manager import PromptManager
from config.settings import settings
import json

class DataSourceManager:
    def __init__(self):
        self.output_dir = settings.output_dir
        self.llm_client = LLMClient()
        self.prompt_manager = PromptManager()

    def _get_data_source_path(self, table_name: str, business_domain: str) -> Path:
        domain_dir = self.output_dir / business_domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        return domain_dir / f"{table_name}.md"

    def exists(self, table_name: str, business_domain: str) -> bool:
        return self._get_data_source_path(table_name, business_domain).exists()

    def create_data_source(self, data: Dict[str, Any], business_domain: str) -> Path:
        """创建新的数据源文件"""
        file_path = self._get_data_source_path(data["table_name"], business_domain)

        content = self._generate_markdown(data)
        file_path.write_text(content, encoding="utf-8")
        return file_path

    def merge_data_source(self, old_content: str, new_data: Dict[str, Any]) -> Tuple[str, List[str]]:
        """合并新旧数据源信息，返回合并后的内容和更新点"""
        # 先生成新数据源的markdown
        new_content = self._generate_markdown(new_data)

        # 调用LLM进行智能合并
        prompt = self.prompt_manager.get_prompt(
            "merge_data_source",
            old_content=old_content,
            new_content=new_content
        )
        merged_content = self.llm_client.chat(prompt)

        # 识别更新点
        update_points = self._detect_update_points(old_content, merged_content)
        return merged_content, update_points

    def update_data_source(self, table_name: str, business_domain: str, new_data: Dict[str, Any]) -> Tuple[Path, List[str]]:
        """更新现有数据源文件"""
        file_path = self._get_data_source_path(table_name, business_domain)
        old_content = file_path.read_text(encoding="utf-8")

        merged_content, update_points = self.merge_data_source(old_content, new_data)
        file_path.write_text(merged_content, encoding="utf-8")
        return file_path, update_points

    def _generate_markdown(self, data: Dict[str, Any]) -> str:
        """生成数据源文件的Markdown内容"""
        md = f"# {data['table_name']}\n\n"
        md += "## 1.数据源基本信息\n\n"
        md += f"### 1.1.数据源名称\n{data.get('name', data['table_name'])}\n\n"
        md += f"### 1.2.数据源描述\n{data.get('description', '')}\n\n"
        md += f"### 1.3.业务域\n{data.get('business_domain', '')}\n\n"

        md += "## 2.数据表结构\n\n"
        md += f"### 2.1.表名\n{data['table_name']}\n\n"
        md += "### 2.2.关键字段\n"
        md += "| 字段名|字段描述 | 用途说明|\n"
        md += "|----------|----------|----------|\n"
        for field in data.get("fields", []):
            md += f"|{field['name']}|{field.get('description', '')}|{field.get('usage', '')}|\n"
        md += "\n"

        md += "## 3.SQL使用示例\n\n"
        for i, example in enumerate(data.get("sql_examples", []), 1):
            md += f"### 3.{i}.{example['name']}\n"
            md += "```sql\n"
            md += example["sql"]
            md += "\n```\n\n"

        md += "## 4.使用说明和注意事项\n\n"
        md += f"### 4.1.使用说明\n{data.get('usage_instructions', '')}\n\n"
        md += f"### 4.2.注意事项\n{data.get('notes', '')}\n\n"

        md += "## 5.数据质量说明\n\n"
        quality = data.get("data_quality", {})
        md += "### 5.1.数据量\n"
        md += f"\t- 日记录数：{quality.get('daily_records', '')}\n"
        md += f"\t- 日覆盖用户数：{quality.get('daily_users', '')}\n\n"
        md += f"### 5.2.数据覆盖情况\n{quality.get('coverage', '')}\n\n"
        md += f"### 5.3.上报及时性\n{quality.get('timeliness', '')}\n\n"

        md += "## 6.关联案例\n"
        md += "|案例名称|案例类型|使用场景|\n"
        md += "|------------|------------|------------|\n"
        for case in data.get("related_cases", []):
            md += f"|{case['name']}|{case.get('type', '')}|{case.get('scenario', '')}|\n"

        return md

    def _detect_update_points(self, old_content: str, new_content: str) -> List[str]:
        """检测更新点，返回更新类型列表"""
        update_points = []

        # 简单的规则检测更新类型
        old_lines = set(old_content.splitlines())
        new_lines = set(new_content.splitlines())
        added_lines = new_lines - old_lines

        added_text = "\n".join(added_lines)

        if "```sql" in added_text:
            update_points.append("新增SQL示例")
        if "字段名" in added_text:
            update_points.append("补充字段说明")
        if "数据质量" in added_text or "日记录数" in added_text:
            update_points.append("更新数据质量信息")
        if "关联案例" in added_text or "案例名称" in added_text:
            update_points.append("新增关联案例")
        if "使用说明" in added_text or "注意事项" in added_text:
            update_points.append("完善使用信息")
        if any(k in added_text for k in ["数据源描述", "字段描述"]):
            update_points.append("修正描述信息")

        if not update_points:
            update_points.append("合并重复信息")

        return update_points
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_data_source_manager.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add src/data_source_manager.py tests/test_data_source_manager.py
git commit -m "feat: implement data source manager with smart merge"
```

---

### Task 7: 解析进度管理器实现

**Files:**
- Create: `src/progress_manager.py`
- Test: `tests/test_progress_manager.py`

**Step 1: 编写进度管理器测试**
```python
# tests/test_progress_manager.py
from src.progress_manager import ProgressManager
from pathlib import Path

def test_progress_initialization():
    manager = ProgressManager()
    assert manager.progress_file.exists()
    assert "待解析文件" in manager.progress_file.read_text()

def test_mark_file_processed():
    manager = ProgressManager()
    test_file = Path("案例/测试/test.md")
    manager.add_pending_file(test_file)
    manager.mark_file_processed(test_file)
    assert test_file.as_posix() in manager.progress_file.read_text()
    assert "- [x]" in manager.progress_file.read_text()
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_progress_manager.py -v`
Expected: FAIL with "ProgressManager not defined"

**Step 3: 实现进度管理器**
```python
# src/progress_manager.py
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
from config.settings import settings

class ProgressManager:
    def __init__(self):
        self.progress_file = settings.output_dir / "解析进度.md"
        self._initialize()

    def _initialize(self):
        """初始化进度文件"""
        if not self.progress_file.exists():
            settings.output_dir.mkdir(parents=True, exist_ok=True)
            content = """# 数据源解析进度

## 待解析文件

### 案例文件（.sql 或者 .md）

## 已解析文件

## 数据源索引
|数据源表名|业务域|文件路径|
|-----------|-----------|-----------|

## 解析记录
|数据源名称|解析时间|操作类型| 更新内容|
|-----------|-----------|-----------|-----------|
"""
            self.progress_file.write_text(content, encoding="utf-8")

    def add_pending_files(self, files: List[Path]):
        """批量添加待解析文件"""
        content = self.progress_file.read_text(encoding="utf-8")
        pending_section = "### 案例文件（.sql 或者 .md）\n"
        lines = content.splitlines()

        # 找到待解析文件部分
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if pending_section in line:
                start_idx = i + 1
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        if start_idx is None:
            return

        # 添加新文件
        existing_files = set()
        for line in lines[start_idx:end_idx]:
            if line.startswith("- [ ] "):
                existing_files.add(line[6:].strip())

        new_lines = []
        for file in files:
            file_path = file.as_posix()
            if file_path not in existing_files:
                new_lines.append(f"- [ ] {file_path}")

        if new_lines:
            lines[start_idx:start_idx] = new_lines
            self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def add_pending_file(self, file: Path):
        """添加单个待解析文件"""
        self.add_pending_files([file])

    def mark_file_processed(self, file: Path):
        """标记文件为已处理"""
        content = self.progress_file.read_text(encoding="utf-8")
        file_path = file.as_posix()

        # 从待解析中移除，添加到已解析
        lines = content.splitlines()
        new_lines = []
        processed = False

        for line in lines:
            if line == f"- [ ] {file_path}":
                processed = True
                continue
            if line == "## 已解析文件" and processed:
                new_lines.append(line)
                new_lines.append(f"- [x] {file_path}")
                processed = False
                continue
            new_lines.append(line)

        self.progress_file.write_text("\n".join(new_lines), encoding="utf-8")

    def add_data_source_index(self, table_name: str, business_domain: str, file_path: Path):
        """添加数据源索引"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # 找到数据源索引表格
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if line == "## 数据源索引":
                start_idx = i + 3  # 跳过表头
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        if start_idx is None:
            return

        # 检查是否已存在
        exists = False
        for line in lines[start_idx:end_idx]:
            if line.startswith(f"|{table_name}|"):
                exists = True
                break

        if not exists:
            relative_path = file_path.relative_to(settings.output_dir).as_posix()
            new_line = f"|{table_name}| {business_domain} |{relative_path}|"
            lines.insert(start_idx, new_line)
            self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def add_parse_record(self, table_name: str, operation_type: str, update_points: List[str] = None):
        """添加解析记录"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        # 找到解析记录表格
        start_idx = None
        for i, line in enumerate(lines):
            if line == "## 解析记录":
                start_idx = i + 3  # 跳过表头
                break

        if start_idx is None:
            return

        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        update_content = "；".join(update_points) if update_points else ""
        new_line = f"|{table_name}| {now}|{operation_type}| {update_content}|"
        lines.insert(start_idx, new_line)

        self.progress_file.write_text("\n".join(lines), encoding="utf-8")

    def get_pending_files(self) -> List[Path]:
        """获取待解析文件列表"""
        content = self.progress_file.read_text(encoding="utf-8")
        lines = content.splitlines()

        pending_section = "### 案例文件（.sql 或者 .md）\n"
        start_idx = None
        end_idx = None
        for i, line in enumerate(lines):
            if pending_section in line:
                start_idx = i + 1
            elif start_idx is not None and line.startswith("## "):
                end_idx = i
                break

        pending_files = []
        if start_idx is not None:
            for line in lines[start_idx:end_idx]:
                if line.startswith("- [ ] "):
                    file_path = Path(line[6:].strip())
                    pending_files.append(file_path)

        return pending_files
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_progress_manager.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add src/progress_manager.py tests/test_progress_manager.py
git commit -m "feat: implement progress manager"
```

---

## 阶段4: 主流程与质量校验

### Task 8: 主流程编排实现

**Files:**
- Create: `src/main.py`
- Test: `tests/test_main.py`

**Step 1: 编写主流程测试**
```python
# tests/test_main.py
from src.main import DataSourceParser
from pathlib import Path

def test_scan_source_files():
    parser = DataSourceParser()
    # 创建测试文件
    test_dir = Path("案例/测试")
    test_dir.mkdir(parents=True, exist_ok=True)
    (test_dir / "test1.md").write_text("# 测试", encoding="utf-8")
    (test_dir / "test2.sql").write_text("SELECT * FROM test", encoding="utf-8")

    files = parser.scan_source_files()
    assert len(files) >= 2
    assert any("test1.md" in f.as_posix() for f in files)
    assert any("test2.sql" in f.as_posix() for f in files)
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_main.py -v`
Expected: FAIL with "DataSourceParser not defined"

**Step 3: 实现主流程编排**
```python
# src/main.py
from pathlib import Path
from typing import List
from config.settings import settings
from src.parser import FileParser
from src.data_source_manager import DataSourceManager
from src.progress_manager import ProgressManager

class DataSourceParser:
    def __init__(self):
        self.parser = FileParser()
        self.data_source_manager = DataSourceManager()
        self.progress_manager = ProgressManager()

    def scan_source_files(self) -> List[Path]:
        """扫描所有待解析的源文件"""
        source_dir = settings.source_dir
        files = []
        for ext in ["*.md", "*.sql"]:
            files.extend(list(source_dir.rglob(ext)))
        return files

    def process_file(self, file_path: Path):
        """处理单个文件"""
        print(f"Processing file: {file_path}")

        # 读取文件内容
        content = file_path.read_text(encoding="utf-8")

        # 解析文件
        if file_path.suffix == ".md":
            parse_result = self.parser.parse_md(content)
        elif file_path.suffix == ".sql":
            parse_result = self.parser.parse_sql(content, file_path.name)
        else:
            print(f"Unsupported file type: {file_path.suffix}")
            return

        business_domain = parse_result.get("business_domain", "其他")
        data_sources = parse_result.get("data_sources", [])

        # 处理每个数据源
        for ds in data_sources:
            ds["business_domain"] = business_domain
            table_name = ds["table_name"]

            if self.data_source_manager.exists(table_name, business_domain):
                # 更新现有数据源
                file_path, update_points = self.data_source_manager.update_data_source(
                    table_name, business_domain, ds
                )
                operation_type = "更新数据源"
                print(f"Updated data source: {table_name}, updates: {update_points}")
            else:
                # 创建新数据源
                file_path = self.data_source_manager.create_data_source(ds, business_domain)
                update_points = []
                operation_type = "新建数据源"
                print(f"Created data source: {table_name}")

            # 更新进度
            self.progress_manager.add_data_source_index(table_name, business_domain, file_path)
            self.progress_manager.add_parse_record(table_name, operation_type, update_points)

        # 标记文件为已处理
        self.progress_manager.mark_file_processed(file_path)
        print(f"Completed processing: {file_path}")

    def run(self):
        """运行完整解析流程"""
        print("Starting data source parsing...")

        # 扫描所有源文件
        all_files = self.scan_source_files()
        print(f"Found {len(all_files)} source files")

        # 添加到待解析列表
        self.progress_manager.add_pending_files(all_files)

        # 获取待处理文件
        pending_files = self.progress_manager.get_pending_files()
        print(f"Pending files: {len(pending_files)}")

        # 逐个处理
        for file in pending_files:
            if file.exists():
                self.process_file(file)
            else:
                print(f"File not found: {file}, skipping")

        print("Parsing completed!")

if __name__ == "__main__":
    parser = DataSourceParser()
    parser.run()
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_main.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add src/main.py tests/test_main.py
git commit -m "feat: implement main workflow orchestration"
```

---

### Task 9: 重复检查与报告生成

**Files:**
- Create: `src/quality_checker.py`
- Test: `tests/test_quality_checker.py`

**Step 1: 编写质量检查测试**
```python
# tests/test_quality_checker.py
from src.quality_checker import QualityChecker

def test_detect_duplicates():
    checker = QualityChecker()
    # 创建测试数据源文件
    from pathlib import Path
    test_dir = Path("数据源/测试")
    test_dir.mkdir(parents=True, exist_ok=True)
    (test_dir / "table1.md").write_text("# table1\n## 1.1.数据源描述\n测试表1", encoding="utf-8")
    (test_dir / "table2.md").write_text("# table1\n## 1.1.数据源描述\n测试表1重复", encoding="utf-8")

    duplicates = checker.detect_duplicates()
    assert len(duplicates) > 0
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_quality_checker.py -v`
Expected: FAIL with "QualityChecker not defined"

**Step 3: 实现质量检查器**
```python
# src/quality_checker.py
from pathlib import Path
from typing import List, Dict, Tuple
from config.settings import settings
from src.data_source_manager import DataSourceManager

class QualityChecker:
    def __init__(self):
        self.output_dir = settings.output_dir
        self.report_file = self.output_dir / "检查报告.md"

    def scan_all_data_sources(self) -> Dict[str, List[Path]]:
        """扫描所有数据源文件，按表名分组"""
        data_sources = {}
        for md_file in self.output_dir.rglob("*.md"):
            if md_file.name in ["解析进度.md", "检查报告.md"]:
                continue
            # 读取第一行获取表名
            first_line = md_file.read_text(encoding="utf-8").splitlines()[0].strip()
            if first_line.startswith("# "):
                table_name = first_line[2:].strip()
                if table_name not in data_sources:
                    data_sources[table_name] = []
                data_sources[table_name].append(md_file)
        return data_sources

    def detect_duplicates(self) -> List[Tuple[str, List[Path]]]:
        """检测重复的数据源"""
        data_sources = self.scan_all_data_sources()
        duplicates = []
        for table_name, files in data_sources.items():
            if len(files) > 1:
                duplicates.append((table_name, files))
        return duplicates

    def detect_missing(self, used_tables: List[str]) -> List[str]:
        """检测遗漏的数据源"""
        data_sources = self.scan_all_data_sources()
        existing_tables = set(data_sources.keys())
        missing = [table for table in used_tables if table not in existing_tables]
        return missing

    def generate_report(self, duplicates: List[Tuple[str, List[Path]]], missing: List[str]) -> Path:
        """生成检查报告"""
        content = "# 数据源检查报告\n\n"

        content += "## 1.重复数据源检测\n"
        if duplicates:
            content += f"发现 {len(duplicates)} 个重复数据源：\n\n"
            for table_name, files in duplicates:
                content += f"### {table_name}\n"
                for f in files:
                    content += f"- {f.relative_to(settings.output_dir)}\n"
                content += "\n"
        else:
            content += "未发现重复数据源\n\n"

        content += "## 2.遗漏数据源检测\n"
        if missing:
            content += f"发现 {len(missing)} 个遗漏的数据源：\n\n"
            for table in missing:
                content += f"- {table}\n"
            content += "\n"
        else:
            content += "未发现遗漏数据源\n\n"

        content += "## 3.完整性评估\n"
        total_tables = len(self.scan_all_data_sources())
        content += f"- 总数据源数量：{total_tables}\n"
        content += f"- 重复数据源数量：{len(duplicates)}\n"
        content += f"- 遗漏数据源数量：{len(missing)}\n"
        completeness = 100 - (len(missing) / max(total_tables + len(missing), 1) * 100)
        content += f"- 完整性：{completeness:.1f}%\n"

        self.report_file.write_text(content, encoding="utf-8")
        return self.report_file

    def run(self, used_tables: List[str] = None):
        """运行质量检查"""
        print("Running quality check...")
        duplicates = self.detect_duplicates()
        missing = self.detect_missing(used_tables or [])
        report_file = self.generate_report(duplicates, missing)
        print(f"Quality check completed. Report saved to: {report_file}")
        return report_file
```

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_quality_checker.py -v`
Expected: PASS

**Step 5: Commit**
```bash
git add src/quality_checker.py tests/test_quality_checker.py
git commit -m "feat: implement quality checker and report generation"
```

---

## 阶段5: 项目配置与使用说明

### Task 10: 项目入口与使用文档

**Files:**
- Create: `run.py`
- Create: `README.md`

**Step 1: 编写项目入口脚本**
```python
# run.py
import sys
from dotenv import load_dotenv
load_dotenv()

from src.main import DataSourceParser
from src.quality_checker import QualityChecker

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "check":
        # 仅运行质量检查
        checker = QualityChecker()
        checker.run()
    else:
        # 运行完整解析流程
        parser = DataSourceParser()
        parser.run()

        # 运行质量检查
        checker = QualityChecker()
        checker.run()

if __name__ == "__main__":
    main()
```

**Step 2: 编写项目README**
```markdown
# SQL数据源解析工程

## 项目介绍
自动解析案例目录下的.sql/.md文件，通过LLM生成标准化的数据源知识索引文件，支持增量更新和智能合并。

## 核心优势
1. **端到端LLM解析**：直接将完整源文件输入LLM，避免中间处理损失信息，解析质量更高
2. **智能合并策略**：更新数据源时采用分块合并，避免上下文窗口爆炸
3. **断点续传**：解析进度自动保存，中断后可继续处理未完成的文件
4. **自动质量校验**：内置重复检测和完整性检查，生成质量报告

## 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置环境变量
```bash
cp .env.example .env
# 编辑.env文件，配置LLM API密钥等信息
```

### 3. 准备案例文件
将需要解析的.sql和.md文件放在`案例/`目录下，按业务域分类存放。

### 4. 运行解析
```bash
python run.py
```

### 5. 仅运行质量检查
```bash
python run.py check
```

## 输出说明
- `数据源/`目录下按业务域分类存放解析后的数据源文件
- `数据源/解析进度.md`：解析进度和记录
- `数据源/检查报告.md`：质量检查报告

## 项目结构
```
├── 案例/                 # 待解析的案例文件目录
├── 数据源/               # 解析结果输出目录
├── config/              # 配置管理
├── prompts/             # LLM Prompt模板
├── src/                 # 源代码
│   ├── main.py          # 主流程
│   ├── parser.py        # 文件解析器
│   ├── llm_client.py    # LLM客户端
│   ├── data_source_manager.py # 数据源管理
│   ├── progress_manager.py    # 进度管理
│   └── quality_checker.py     # 质量检查
├── tests/               # 单元测试
├── run.py               # 项目入口
├── requirements.txt     # 依赖
└── .env                 # 环境变量配置
```

## 优化点
- 支持批量处理时的并发控制
- 增加解析结果的人工审核流程
- 支持更多类型的源文件解析
- 可扩展支持其他LLM模型
```

**Step 3: Commit**
```bash
git add run.py README.md
git commit -m "feat: add entry script and documentation"
```

---

## 阶段6: 方案补齐（关键缺口修复）

### Task 11: 上下文预算与分块合并落地

**Files:**
- Create: `src/token_budget.py`
- Create: `prompts/merge_section.j2`
- Modify: `src/data_source_manager.py`
- Test: `tests/test_token_budget.py`
- Test: `tests/test_section_merge.py`

**Step 1: 编写失败测试（预算与分块）**
```python
# tests/test_token_budget.py
from src.token_budget import TokenBudget

def test_estimate_tokens_and_fit_budget():
    budget = TokenBudget(max_tokens=8000, reserve_tokens=1500)
    assert budget.estimate_tokens("a" * 4000) > 0
    assert budget.can_fit(["a" * 1000, "b" * 1000]) is True
```

```python
# tests/test_section_merge.py
from src.data_source_manager import DataSourceManager

def test_merge_only_changed_sections():
    manager = DataSourceManager()
    old_md = "# t\n\n## 1.数据源基本信息\nA\n\n## 3.SQL使用示例\nS1"
    new_data = {"table_name": "t", "description": "A", "sql_examples": [{"name": "x", "sql": "select 1"}]}
    merged, points = manager.merge_data_source(old_md, new_data)
    assert "select 1" in merged
    assert "新增SQL示例" in points
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_token_budget.py tests/test_section_merge.py -v`  
Expected: FAIL with `TokenBudget not defined` or `section merge not implemented`

**Step 3: 实现预算器与分块合并**
```python
# src/token_budget.py
from dataclasses import dataclass
from typing import List

@dataclass
class TokenBudget:
    max_tokens: int
    reserve_tokens: int = 1500

    @property
    def usable_tokens(self) -> int:
        return max(self.max_tokens - self.reserve_tokens, 1)

    def estimate_tokens(self, text: str) -> int:
        # 中文/SQL混合场景下使用保守估算，避免超窗
        return max(len(text) // 2, 1)

    def can_fit(self, blocks: List[str]) -> bool:
        return sum(self.estimate_tokens(b) for b in blocks) <= self.usable_tokens
```

Task 11实现要求：
1. 将旧数据源文档拆成一级模块（`## 1` 到 `## 6`），对比新内容仅挑出变更模块；
2. 使用 `merge_section.j2` 按模块调用LLM，禁止把完整旧文+完整新文一次性送入；
3. `TokenBudget` 先做预算检查，超预算时进一步拆分（如 SQL 示例逐条合并）；
4. 合并完成后再拼装完整文档，保留原有章节顺序。

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_token_budget.py tests/test_section_merge.py -v`  
Expected: PASS

**Step 5: Commit**
```bash
git add src/token_budget.py prompts/merge_section.j2 src/data_source_manager.py tests/test_token_budget.py tests/test_section_merge.py
git commit -m "feat: implement token budget manager and section-level merge"
```

---

### Task 12: 案例用表索引与遗漏检测闭环

**Files:**
- Create: `src/table_usage_tracker.py`
- Modify: `src/main.py`
- Modify: `src/quality_checker.py`
- Modify: `src/progress_manager.py`
- Test: `tests/test_table_usage_tracker.py`
- Test: `tests/test_quality_checker_missing.py`

**Step 1: 编写失败测试（遗漏检测闭环）**
```python
# tests/test_quality_checker_missing.py
from src.quality_checker import QualityChecker

def test_detect_missing_with_usage_index():
    checker = QualityChecker()
    missing = checker.detect_missing(["dwd.a", "dwd.b"])
    assert isinstance(missing, list)
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_quality_checker_missing.py -v`  
Expected: FAIL with usage index not wired

**Step 3: 实现闭环**
Task 12实现要求：
1. `table_usage_tracker.py` 持久化 `案例 -> 使用表集合`（建议 JSON 文件：`数据源/案例表使用索引.json`）；
2. `main.py` 在处理每个源文件后，将该文件提取到的表名写入 tracker；
3. `quality_checker.py` 默认读取 tracker 作为 `used_tables`，不再依赖外部手工传参；
4. `progress_manager.py` 在 `解析进度.md` 增加“案例表使用索引文件路径”说明行，便于审计。

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_table_usage_tracker.py tests/test_quality_checker_missing.py -v`  
Expected: PASS

**Step 5: Commit**
```bash
git add src/table_usage_tracker.py src/main.py src/quality_checker.py src/progress_manager.py tests/test_table_usage_tracker.py tests/test_quality_checker_missing.py
git commit -m "feat: close missing-data-source detection loop with usage tracker"
```

---

### Task 13: 主流程与进度管理关键缺陷修复

**Files:**
- Modify: `src/main.py`
- Modify: `src/progress_manager.py`
- Test: `tests/test_main.py`
- Test: `tests/test_progress_manager.py`

**Step 1: 编写失败测试（路径与待处理列表）**
```python
def test_mark_processed_uses_source_file_path():
    # 断言 mark_file_processed 入参是案例源文件路径，而不是数据源输出路径
    ...
```

```python
def test_get_pending_files_can_parse_section():
    # 断言可以正确读取“### 案例文件（.sql 或者 .md）”下的所有 - [ ] 记录
    ...
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_main.py tests/test_progress_manager.py -v`  
Expected: FAIL with current path-overwrite/section-parse issue

**Step 3: 修复实现**
Task 13实现要求：
1. `main.py` 中避免 `file_path` 变量覆盖：源文件路径使用 `source_file_path`，数据源输出路径使用 `ds_file_path`；
2. `progress_manager.py` 的 section 匹配使用无换行的稳定字符串（不要拼 `\n`）；
3. `mark_file_processed` 仅接收源文件路径，且能幂等处理重复调用。

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_main.py tests/test_progress_manager.py -v`  
Expected: PASS

**Step 5: Commit**
```bash
git add src/main.py src/progress_manager.py tests/test_main.py tests/test_progress_manager.py
git commit -m "fix: correct source-path marking and progress section parsing"
```

---

### Task 14: 更新点识别改为结构化Diff

**Files:**
- Create: `src/update_diff.py`
- Modify: `src/data_source_manager.py`
- Test: `tests/test_update_diff.py`

**Step 1: 编写失败测试（更新点分类）**
```python
from src.update_diff import detect_update_points

def test_detect_multiple_update_points():
    old = {"fields": [{"name": "did"}], "sql_examples": []}
    new = {"fields": [{"name": "did"}, {"name": "uid"}], "sql_examples": [{"name": "ex1", "sql": "select 1"}]}
    points = detect_update_points(old, new)
    assert "新增SQL示例" in points
    assert "补充字段说明" in points
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_update_diff.py -v`  
Expected: FAIL with `detect_update_points not defined`

**Step 3: 实现结构化Diff**
Task 14实现要求：
1. 将更新点识别输入从“文本行差异”改为“结构化对象差异”；
2. 严格映射到需求给定类别：`新增SQL示例`、`补充字段说明`、`更新数据质量信息`、`新增关联案例`、`完善使用信息`、`修正描述信息`、`合并重复信息`；
3. 输出顺序稳定，多个更新点使用分号连接，便于 `解析进度.md` 记录。

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_update_diff.py -v`  
Expected: PASS

**Step 5: Commit**
```bash
git add src/update_diff.py src/data_source_manager.py tests/test_update_diff.py
git commit -m "feat: implement structured update-point diff detection"
```

---

### Task 15: 需求对齐与工程一致性修复

**Files:**
- Modify: `requirements.txt`
- Modify: `config/settings.py`
- Modify: `src/main.py`
- Modify: `src/quality_checker.py`
- Modify: `README.md`
- Test: `tests/test_settings.py`
- Test: `tests/test_quality_checker.py`

**Step 1: 编写失败测试（一致性）**
```python
def test_report_file_name_matches_requirement():
    # 需求为“检察报告.md”
    ...
```

**Step 2: 运行测试验证失败**
Run: `pytest tests/test_settings.py tests/test_quality_checker.py -v`  
Expected: FAIL with dependency/file-name/domain-init mismatch

**Step 3: 对齐实现**
Task 15实现要求：
1. `requirements.txt` 增加 `pydantic-settings>=2.0.0`；
2. 初始化阶段增加业务域目录准备：先创建已知业务域目录；未知业务域按首批解析结果动态创建；
3. 质量报告文件统一为 `数据源/检察报告.md`（代码、README、输出说明全部对齐）；
4. 保持“单源文件 <=1.5K 行时直接全量输入 LLM 解析”的策略，不引入SQL规则预抽取。

**Step 4: 运行测试验证通过**
Run: `pytest tests/test_settings.py tests/test_quality_checker.py -v`  
Expected: PASS

**Step 5: Commit**
```bash
git add requirements.txt config/settings.py src/main.py src/quality_checker.py README.md tests/test_settings.py tests/test_quality_checker.py
git commit -m "chore: align dependencies and outputs with requirement spec"
```

---

## 补齐后验收标准

1. 任一已存在数据源的更新流程不再进行“整文合并”，必须命中分块合并与预算控制；
2. `python run.py` 后，`数据源/检察报告.md` 必须包含重复/遗漏/完整性三部分；
3. 遗漏检测不依赖人工传参，能直接从案例用表索引得到结果；
4. `解析进度.md` 中待解析、已解析、索引、解析记录四部分在多次运行后结构稳定；
5. 更新点记录必须只使用需求定义的类别，且多个类别按分号拼接。

---

Plan complete and saved to `docs/plans/2026-03-08-sql-data-source-parser.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

Which approach?
