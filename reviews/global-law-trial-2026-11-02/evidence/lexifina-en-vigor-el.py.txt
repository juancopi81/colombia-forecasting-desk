"""Prints the law of Colombia as it stood on a given day, read from this repository's history.

    python tools/en_vigor_el.py 2004-08-29 constitucion 15        # art. 15 the day before Sentencia C-816 de 2004
    python tools/en_vigor_el.py 2020-07-22 constitucion 34        # art. 34 as Acto Legislativo 1 de 2020 left it
    python tools/en_vigor_el.py 2015-07-01 constitucion 257a      # an added article, by its number and letter
    python tools/en_vigor_el.py 2000-01-01 constitucion           # the whole Constitution on 1 January 2000
    python tools/en_vigor_el.py --law 2021-09-02 constitucion 34  # also name the law or ruling behind that wording

A thin wrapper around tools/on.py: the version is chosen by each commit's `In-force:` trailer, never by its commit
date, and branches named wip* are ignored. A bare article number (`15`, `22a`) means the article of the body of the
act (`art-015`, `art-022a`), not a transitional article with the same number. Standard library only.
"""
import re
import runpy
import subprocess
import sys
from pathlib import Path


def article(act: str, unit: str) -> str:
    m = re.fullmatch(r'(\d+)([a-z]?)', unit.lower())
    if not m:
        return unit
    files = set()
    for ref in ('main', 'futuro'):
        out = subprocess.run(['git', 'ls-tree', '-r', '--name-only', ref, f'{act}/'], capture_output=True,
                             text=True, encoding='utf-8')
        files.update(out.stdout.split())
    for width in (3, 4):
        key = f'art-{int(m.group(1)):0{width}d}{m.group(2)}'
        if f'{act}/{key}.md' in files:
            return key
    return unit


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    args = [a for a in sys.argv[1:] if a != '--law']
    if len(args) == 3:
        i = len(sys.argv) - 1 - sys.argv[::-1].index(args[2])
        sys.argv[i] = article(args[1], args[2])
    sys.argv[0] = str(Path(__file__).with_name('on.py'))
    runpy.run_path(sys.argv[0], run_name='__main__')
