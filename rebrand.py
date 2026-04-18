#!/usr/bin/env python3
# Run from ~/Documents/Poist.ie:  python3 rebrand.py
import re, os
from pathlib import Path

LOGO_LIGHT = "assets/poist-logo-trans.png"
LOGO_DARK  = "assets/poist-logo-trans.png"

NAV_NEW  = f'<a href="index.html" class="nav-logo"><img src="{LOGO_LIGHT}" alt="poist.ie" style="height:48px;width:auto;display:block;mix-blend-mode:screen;"></a>'
FOOT_NEW = f'<div class="foot-logo"><img src="{LOGO_DARK}" alt="poist.ie" style="height:38px;width:auto;filter:brightness(0) invert(1);"></div>'

SKIP = {"coming-soon.html", "404.html"}
root = Path(os.getcwd())
files = sorted(f for f in root.glob("*.html") if f.name not in SKIP)
updated, unchanged = [], []

for f in files:
    text = f.read_text(encoding="utf-8")
    orig = text
    # Nav logo (any attribute order, any inner content)
    text = re.sub(r'<a\b[^>]*\bclass="nav-logo[^"]*"[^>]*>.*?</a>', NAV_NEW, text, flags=re.DOTALL)
    text = re.sub(r'<a\b[^>]*\bclass="logo[^"]*"[^>]*href="[^"]*"[^>]*>.*?</a>', NAV_NEW, text, flags=re.DOTALL)
    # Footer logo
    text = re.sub(r'<div\s+class="foot-logo">.*?</div>', FOOT_NEW, text, flags=re.DOTALL)
    if text != orig:
        f.write_text(text, encoding="utf-8")
        updated.append(f.name)
    else:
        unchanged.append(f.name)

print(f"\n✅  Updated {len(updated)} files:")
for n in updated: print(f"   {n}")
print(f"\n⏭   Unchanged {len(unchanged)} files:")
for n in unchanged: print(f"   {n}")