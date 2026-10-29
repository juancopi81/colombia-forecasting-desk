# Write the daily Spanish X draft

After the reader brief is finalized (on Fridays, after the weekly brief), write
`runs/<date>/x_post_es.md`: up to three short X posts in Spanish for the general
public. Juan approved quoting the desk's internal calls here. He reviews the
draft and posts it by hand. Never post, open X or use a browser for X.

Use only what today's finalized brief and the two ledgers already establish.
Copy probabilities, thresholds, dates and official links exactly; never create,
round or update a call here, and add no new claims.

Include, in this order and only when present:

1. Results: calls resolved since the most recent earlier draft, hits and misses
   alike. Give the official outcome, our probability and whether the outcome
   happened.
2. Upcoming calls: open calls whose official event falls after today and within
   seven days. Name the event, the reference value and the estimated
   probability of the stated outcome.
3. Public actions with an official date and link: comment periods, voter or
   program registrations, application windows and filing deadlines. Say who
   can act and where. For consultations, say that the issuing agency takes
   comments from anyone, so readers don't mistake it for replying on X.

Leave out calls whose event has passed but whose record is pending (they become
results once resolved), watchlist or procurement patterns about named companies
or people, data problems, anything addressed to Juan, and any investment,
trading, borrowing or betting language. Skip items already in any
`runs/*/x_post_es.md` from the previous seven days unless their status changed.
If nothing remains, write exactly `Hoy no hay borrador para X.`

Write plain Colombian Spanish: decimal comma and `%` without a space (12,25%),
dates such as "viernes 30 de octubre", and the word "probabilidad" next to every
probability so it cannot be read as the indicator's value. Separate posts with
a line containing only `---`. End every post with
`🤖 Hecho con IA · No es asesoría financiera` and link the official source when
it fits.

Each post must fit X's 280 limit as X counts it: every URL counts 23;
characters in U+0000–U+10FF, U+2000–U+200D, U+2010–U+201F and U+2032–U+2037
count 1; everything else, including emoji and •, counts 2. Check every post with
a short Python script and shorten any that fail.

In the final message, after the brief, add a line `---`, then
`**X draft (Spanish), review before posting:**` and each post in its own `text`
code block. If there is no draft, add `X draft: none today.` instead.

Example of an upcoming-calls post:

    📅 Viernes 30 de octubre. Probabilidad estimada:

    • Que el Banco de la República suba su tasa de interés (hoy 12,25%): 65%
    • Que el desempleo de septiembre baje del 8,2% de hace un año (lo publica el DANE): 35%

    🤖 Hecho con IA · No es asesoría financiera

Example of a results post:

    Resultado de nuestras probabilidades:

    • Desempleo de septiembre (DANE): [valor oficial]. Le dimos 35% de probabilidad a que bajara del 8,2%, y [sí ocurrió / no ocurrió].

    🤖 Hecho con IA · No es asesoría financiera

Example of a consultation post:

    📝 Consulta pública hasta el 3 de noviembre: MinHacienda recibe comentarios de cualquier persona sobre un borrador de decreto para cooperativas de ahorro y crédito (servicios compartidos y corresponsales).
    https://www.minhacienda.gov.co/normativa/proyectos-de-decretos/2026
    🤖 Hecho con IA · No es asesoría financiera

Example of a public-action post:

    📍 ¿Te cambiaste de casa? Desde este sábado 31 de octubre puedes inscribir tu cédula en un nuevo puesto de votación para las elecciones de alcaldes, gobernadores, concejos y asambleas del 31 de octubre de 2027.
    https://www.registraduria.gov.co/Registraduria-Nacional-fijo-el-calendario-electoral-para-las-elecciones.html
    🤖 Hecho con IA · No es asesoría financiera
