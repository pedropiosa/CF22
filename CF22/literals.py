raw_cols = ["Entity", "EPNumber", "Type", "Date", "Lang", "Section", "Published", "Detail"]
proc_cols = ["EPDocNumber", "Date", "Title", "Abstr", "Descr", "Entity", "IDNumber", "Lang", "cpc"]

patterns = [("green", "plastic"),
            ("alternat", "plastic"),
            ("green", "polymer"),
            ("alternat", "polymer"),
            ("plastic", "environment"),
            ("polymer", "environment"),
            ("plastic", "wast"),
            ("polymer", "wast"),
            ("plastic", "recycl"),
            ("polymer", "recycl"),
            ("plastic", "sustainab"),
            ("polymer", "sustainab"),
            ("plastic", "pollut"),
            ("bio.?degrad", "plastic"),
            ("bio.?degrad", "polymer"),
            ("re.?usab", "plastic"),
            ("chem", "recycl"),
            ("biolog", "recycl"),
            ("mech", "recycl"),
            ("re.?usab", "polymer"),
            ("PET ", "recycl"),
            ("compostabl", "plastic"),
            ("compostabl", "polymer"),
            ("virgin", "plastic"),
            ("green.?house", "polymer"),
            ("green.?house", "plastic"),
            ("RPC", "recycl"),
            ("up.?cycl", "plastic"),
            ("up.?cycl", "polymer"),
            ("reclaim", "rubber"),
            ("reclaim", "plastic"),
            ("renewabl", "plastic"),
            ("renewabl", "polymer")]

format = "csv"
write_mode = "overwrite"