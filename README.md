# Wikipedia Links

The goal of this project is to find a path of links between two given Wikipedia
pages. 

## Data

Recent [Wikipedia Dumps](https://dumps.wikimedia.org/enwiki/) are first
downloaded. The `pagelinks`, `page` and `redirects` tables are required. 

The `pagelinks` table maps all links between Wikipedia pages. The `page` table 
matches page titles to their ID. However, some pages redirect elsewhere, so the
`redirects` table is used to resolve these links.
