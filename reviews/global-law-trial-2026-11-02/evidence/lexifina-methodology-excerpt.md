## Source

The text and every date come from the **Secretaría General del Senado**'s "basedoc" collection ([secretariasenado.gov.co/senado/basedoc](http://www.secretariasenado.gov.co/senado/basedoc/constitucion_politica_1991.html)), compiled by Avance Jurídico and updated to 15 October 2026 (Diario Oficial No. 53.651). It is the only free official site that keeps, for each provision, the earlier texts ("Legislación Anterior": "Texto original de la Constitución Política", "Texto modificado por el Acto Legislativo 2 de 2003"), notes naming the act that changed it with its Diario Oficial date ("Notas de Vigencia"), and the Constitutional Court rulings on it ("Jurisprudencia Vigencia").

The source has **no dated snapshots**. Each version here is rebuilt from those earlier texts and from the source's inline amendment markers ("<Inciso adicionado por el artículo 1 del Acto Legislativo 1 de 2005 ...>"). Today's text in the repository was checked against the source's current text, provision by provision, on 28 October 2026: all 496 provisions match (`tools/verify.py`).

## Dates

- Each commit is dated at **noon, America/Bogota, on the day the change took effect**. Enactment and publication dates are trailers (`Enacted:`, `Published:`); the in-force day is in `In-force:`, which the tools read.
- **Actos Legislativos** take effect on the day of their publication in the Diario Oficial, as the source's notes say ("Rige a partir de su publicación"), unless the note quotes another vigencia clause: Acto Legislativo 1 de 2001 from 1 January 2002; Actos Legislativos 1 and 4 de 2007 from 1 January 2008; and the clauses tied to elections, Acto Legislativo 2 de 2005 (12 March 2006), 3 de 2005 (14 March 2010) and 3 de 2007 (28 October 2007). The election days come from the Registraduría Nacional del Estado Civil, read from pages by Lexifina's code: its [Calendario Electoral 2006 - 2010](https://www.registraduria.gov.co/Calendario-Electoral-2006-2010.html) (Resolución 0497 de 2005), its [Comunicado de Prensa No. 050 de 2009](https://web.archive.org/web/20110726040247id_/http://www.registraduria.gov.co/ESTA-LISTO-EL-PRE-CALENDARIO.html) (Internet Archive copy), and its [home page of 30 September 2007](https://web.archive.org/web/20070930160642id_/http://www.registraduria.gov.co/) ("el proceso electoral del 28 de octubre"; the year is the capture's). Each of those commits names its source in the message.
- **Default rule** for ordinary laws: Ley 4 de 1913 (Código de Régimen Político y Municipal), arts. 52 and 53: a law binds two months after promulgation unless it fixes its own date.
- **The Constitution of 1991** took effect on its promulgation (art. 380), in Gaceta Constitucional No. 114 of 4 July 1991. The source gives the text of the second corrected edition (Gaceta No. 116 of 20 July 1991), which is the first commit; the corrections of the Aclaración published in Gaceta No. 125 (25 September 1991) are a commit of their own.
- **Court rulings** (Corte Constitucional) are dated by the day of the ruling, as the source gives it.
- `--before="YYYY-MM-DD"` alone uses your clock's time of day, so a change that took effect that day may be missed before noon. Use `T23:59:59Z` as above, or the tool.

## What "author" means

The author of a commit is the act or ruling that made the change, with an e-mail address that cannot exist (`al-2-2015@secretariasenado.invalid`), so `git blame` and `git log --author` name them. The committer is always Lexifina.

- **Credits come only from the source.** A change is credited to an Acto Legislativo only when the source's earlier texts, markers or notes name it.
- **Constitutional Court rulings are authors too** (kind `court`). When the Court struck a whole Acto Legislativo (for example Sentencia C-816 de 2004 on Acto Legislativo 2 de 2003, C-740 de 2013 on Acto Legislativo 2 de 2012, C-294 de 2021 on Acto Legislativo 1 de 2020), the provision returns, on the ruling's day, to its text before that act. When it struck words ("<Aparte tachado INEXEQUIBLE>"), the struck words are in the text until the ruling and gone after it.
- Blame credits whole lines (one paragraph per line). Use `git log -p --word-diff` to see exactly which words each act changed.

## What it does not show

- **Gaps in the source.** For a few provisions the source names an act that changed them but gives no text for the period after it (a later act rewrote the same words, and only the latest text is kept). There the file reads `[La fuente no da el texto vigente en esta fecha.]` for that period, credited to the act named; nothing is guessed. The cases (arts. 122, 256, 263, 322, 328 and 361) are listed in `REPRODUCE.md`. Art. 263 also holds the text of the former art. 263-A, which Acto Legislativo 2 de 2015 moved there.
- **Rulings that change no words.** Plain EXEQUIBLE rulings, inhibitions and "estarse a lo resuelto" rulings are not recorded.
- **Avance Jurídico's editorial notes**, concordances and commentary are not in the text; the notes are used only to date and credit changes.
- **The first printing** of the Constitution (Gaceta No. 114) is not in the source; the history starts with the corrected text of Gaceta No. 116.

## How it is built

Lexifina reads the sources listed here and records each act's dated versions, and the laws that made each change, in one corpus file. `tools/build.py` writes this repository from that file, and the same corpus always gives the same commit hashes; `REPRODUCE.md` gives the details and the checks. A model (Claude) wrote Lexifina's code; it wrote no legal text, and no model runs when the history is built.

No AI model wrote or changed legal text. Claude Opus 5.5 wrote the scripts and chose the sources; see `REPRODUCE.md`.

## Sources

| Source | Kind | Past versions | Names the amending act | Bulk | Notes |
| --- | --- | --- | --- | --- | --- |
| [Secretaría General del Senado, basedoc](http://www.secretariasenado.gov.co/senado/basedoc/) | Official legislation (compiled by Avance Jurídico) | earlier texts per provision, no dated snapshots | yes, per provision, with Diario Oficial dates and Court rulings | per document (HTML pages plus one JavaScript file per page holding the note boxes) | Used |
| [SUIN-Juriscol](https://www.suin-juriscol.gov.co) (Ministerio de Justicia) | Official legislation | no dated versions seen | yes ("Adicionado por: Art. 2 Ley 1482 de 2011"), with Court rulings attached | no | An Angular app that serves no text to a plain request; it does not keep earlier texts. Not used |
| [Registraduría Nacional del Estado Civil](https://www.registraduria.gov.co) | Electoral authority | n/a | n/a | no | Used only for the days of three elections that Actos Legislativos take effect on (two pages through Internet Archive captures pinned by timestamp) |
| Diario Oficial (Imprenta Nacional) | Official gazette | n/a | n/a | no | Not read; the source gives the Diario Oficial number and date of every act |

Research guides that cover every country: [GlobaLex](https://www.nyulawglobal.org/globalex/) and the [Law Library of Congress Guide to Law Online](https://www.loc.gov/research-centers/law-library-of-congress/researcher-resources/guide-to-law-online/).

## Licence

The Constitution, laws and judicial decisions of Colombia may be reproduced freely (Ley 23 de 1982, art. 41), provided the official text is reproduced faithfully. Fuente: Secretaría General del Senado de la República, www.secretariasenado.gov.co. The arrangement, tables, commit messages and tools are by Lexifina, under the terms in `LICENSE`.

This repository is not legal advice and not an official publication.

