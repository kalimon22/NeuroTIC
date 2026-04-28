import os
import re
import yaml
from datetime import datetime

class WikiManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.wiki_dir = os.path.join(base_dir, "wiki")
        self.raw_dir = os.path.join(base_dir, "raw")
        self.index_path = os.path.join(base_dir, "index.md")
        self.log_path = os.path.join(base_dir, "log.md")
        self.schema_path = os.path.join(base_dir, "SCHEMA.md")
        
        # Ensure directories exist
        os.makedirs(self.wiki_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)
        
        self.setup_basic_files()

    def setup_basic_files(self):
        """Initializes index.md, log.md and SCHEMA.md if they don't exist."""
        if not os.path.exists(self.index_path):
            with open(self.index_path, "w", encoding="utf-8") as f:
                f.write("# Wiki Index\n\nCatalog of all knowledge pages.\n\n| Page | Summary | Links |\n| --- | --- | --- |\n")
        
        if not os.path.exists(self.log_path):
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write("# Wiki Log\n\nChronological record of operations.\n\n")

        if not os.path.exists(self.schema_path):
            with open(self.schema_path, "w", encoding="utf-8") as f:
                f.write("# SCHEMA & AGENT GUIDELINES\n\nThis wiki is a structured collection of knowledge. Use the following rules:\n"
                        "1. Every concept has its own page in `wiki/Concept.md`.\n"
                        "2. Pages must include YAML frontmatter for structured relations.\n"
                        "3. Use `[[Concept]]` for internal WikiLinks.\n"
                        "4. Keep narrative descriptions concise and factual.\n")

    def get_page_path(self, concept):
        # Sanitize filename
        safe_name = re.sub(r'[\\/*?:"<>|]', "", concept)
        return os.path.join(self.wiki_dir, f"{safe_name}.md")

    def write_page(self, concept, content, metadata=None):
        """Writes a markdown page with optional YAML frontmatter."""
        path = self.get_page_path(concept)
        
        frontmatter = ""
        if metadata:
            # We skip pyyaml for writes to keep it simple or use it if present
            try:
                import yaml
                fm_text = yaml.dump(metadata, sort_keys=False, allow_unicode=True)
                frontmatter = f"---\n{fm_text}---\n\n"
            except ImportError:
                # Fallback manual dump (simple key-value)
                fm_text = "\n".join([f"{k}: {v}" for k, v in metadata.items()])
                frontmatter = f"---\n{fm_text}\n---\n\n"

        full_content = f"{frontmatter}{content}"
        with open(path, "w", encoding="utf-8") as f:
            f.write(full_content)
        
        self.log_action(f"ingest | {concept}")
        self.update_index_entry(concept, metadata.get("description", "") if metadata else "")

    def read_page(self, concept):
        path = self.get_page_path(concept)
        if not os.path.exists(path):
            return None, None
        
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        
        # Parse YAML
        metadata = {}
        content = text
        if text.startswith("---"):
            parts = text.split("---", 2)
            if len(parts) >= 3:
                try:
                    import yaml
                    metadata = yaml.safe_load(parts[1]) or {}
                except (ImportError, Exception):
                    # Manual basic parse
                    for line in parts[1].strip().split("\n"):
                        if ":" in line:
                            k, v = line.split(":", 1)
                            metadata[k.strip()] = v.strip()
                content = parts[2].strip()
        
        return content, metadata

    def log_action(self, action_msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"## [{timestamp}] {action_msg}\n")

    def update_index_entry(self, concept, summary):
        """Updates or adds an entry in index.md."""
        # Simple implementation: read all lines, check if concept exists, replace or append
        if not os.path.exists(self.index_path):
            self.setup_basic_files()
            
        with open(self.index_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        new_line = f"| [[{concept}]] | {summary[:100]} | [File](./wiki/{concept.replace(' ', '%20')}.md) |\n"
        found = False
        for i, line in enumerate(lines):
            if f"[[{concept}]]" in line:
                lines[i] = new_line
                found = True
                break
        
        if not found:
            lines.append(new_line)
            
        with open(self.index_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def list_pages(self):
        return [f.replace(".md", "") for f in os.listdir(self.wiki_dir) if f.endswith(".md")]
