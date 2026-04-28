from __future__ import annotations

STOPWORDS_ES: frozenset[str] = frozenset(
    {
        "a", "al", "algo", "algun", "alguna", "algunas", "alguno", "algunos",
        "ante", "antes", "aqui", "asi", "aunque", "bajo", "bien", "cada",
        "casi", "como", "con", "contra", "cual", "cuales", "cuando", "cuanto",
        "de", "del", "desde", "donde", "durante", "e", "el", "ella", "ellas",
        "ellos", "en", "entre", "era", "eran", "eres", "es", "esa", "esas",
        "ese", "eso", "esos", "esta", "estaba", "estaban", "estado", "estan",
        "estar", "estas", "este", "esto", "estos", "estoy", "fue", "fueron",
        "ha", "han", "has", "hasta", "hay", "haya", "he", "hubo", "la", "las",
        "le", "les", "lo", "los", "ma", "mas", "me", "mi", "mis", "mucho",
        "muchos", "muy", "nada", "ni", "no", "nos", "nosotras", "nosotros",
        "nuestra", "nuestras", "nuestro", "nuestros", "o", "os", "otra",
        "otras", "otro", "otros", "para", "pero", "poco", "por", "porque",
        "que", "qué", "quien", "quienes", "se", "sea", "sean", "segun", "ser",
        "si", "sido", "siempre", "sin", "sobre", "sois", "solo", "somos",
        "son", "soy", "su", "sus", "tambien", "tan", "tanto", "te", "ti",
        "tiene", "tienen", "todo", "todos", "tras", "tu", "tus", "un", "una",
        "unas", "uno", "unos", "vosotras", "vosotros", "vuestra", "vuestras",
        "vuestro", "vuestros", "y", "ya", "yo",
        # english spillovers commonly seen in feed boilerplate
        "the", "and", "or", "of", "for", "to", "in", "on", "by", "with",
        "from", "is", "are", "was", "were", "be", "been", "being", "an",
        # date/calendar noise
        "lunes", "martes", "miercoles", "jueves", "viernes", "sabado",
        "domingo", "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    }
)
