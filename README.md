["Build Your Own SQLite" Challenge](https://codecrafters.io/challenges/sqlite).


# Build Your Own SQLite

Driver to read directly from a `.db` file encoded using the [SQLite Database File Format](https://www.sqlite.org/fileformat.html).

# Usage

```bash
$> ./your_sqlite3.sh superheroes.db "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'"

3913|Matris Ater Clementia (New Earth)
3289|Angora Lapin (New Earth)
2729|Thrust (New Earth)
1085|Felicity (New Earth)
790|Tobias Whale (New Earth)
297|Stealth (New Earth)
```

# TODO

- Support for Btree index
- Logic in where clauses
