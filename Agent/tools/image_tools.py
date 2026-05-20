from pathlib import Path
from typing import Any, Dict


def image_analyze(args: Dict[str, Any]) -> Dict[str, Any]:
    image_path = str(args.get("image_path", "")).strip()
    task = str(args.get("task", "describe")).strip() or "describe"
    if not image_path:
        return {"ok": False, "error": "missing image_path"}

    path = Path(image_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent.parent / image_path

    if not path.exists():
        return {"ok": False, "error": "image not found", "image_path": str(path)}

    return {
        "ok": True,
        "task": task,
        "image_path": str(path),
        "note": "当前仓库未接入真实视觉模型，这里返回占位结果。若需要，可后续把该工具接到单独的多模态 API。",
    }
