Используя Hive найти (команды и результаты записать в файл и добавить в репозиторий):
1. Исполнителя с максимальным числом скробблов - 5 баллов
```SQL
SELECT artist_lastfm FROM artists 
WHERE scrobbles_lastfm IN (
	SELECT MAX(scrobbles_lastfm) FROM artists
);
```
```
artist_lastfm
The Beatles
```
2. Самый популярный тэг на ластфм - 10 баллов
```SQL
SELECT top_tag, COUNT(*) AS cnt FROM artists
LATERAL VIEW EXPLODE(SPLIT(tags_lastfm, ";")) tags_lastfm AS top_tag
GROUP BY top_tag
HAVING top_tag <> ''
ORDER BY cnt DESC
LIMIT 1;
```
```
top_tag	cnt
seen live	81278
```
3. Самые популярные исполнители 10 самых популярных тегов ластфм - 10 баллов

```SQL
WITH top_tags AS (
    SELECT top_tag, COUNT(*) AS cnt FROM artists
    LATERAL VIEW EXPLODE(SPLIT(tags_lastfm, ";")) tags_lastfm AS top_tag
    GROUP BY top_tag
    HAVING top_tag <> ''
    ORDER BY cnt DESC
    LIMIT 10
), artist_data AS (
    SELECT artist_lastfm, scrobbles_lastfm, tags FROM artists
    LATERAL VIEW EXPLODE(SPLIT(tags_lastfm, ";")) tags_lastfm AS tags
)
SELECT artist_data.artist_lastfm, MAX(artist_data.scrobbles_lastfm) AS scrobbles
FROM artist_data
JOIN top_tags ON top_tags.top_tag = artist_data.tags
GROUP BY artist_data.artist_lastfm
ORDER BY scrobbles DESC
LIMIT 10;
```
```
artist_data.artist_lastfm	scrobbles
The Beatles	517126254
Radiohead	499548797
Coldplay	360111850
Muse	344838631
Arctic Monkeys	332306552
Pink Floyd	313236119
Linkin Park	294986508
Red Hot Chili Peppers	293784041
Lady Gaga	285469647
Metallica	281172228
```

4. Любой другой инсайт на ваше усмотрение - 10 баллов
Топ слов в назвониях российских исполнителей. Встречаются в основном имена, куча Александров.
```SQL
SELECT  top_word, COUNT(*) AS cnt FROM artists
LATERAL VIEW EXPLODE(SPLIT(LOWER(artist_lastfm), " ")) artist_lastfm AS top_word
WHERE country_mb = "Russia"
GROUP BY top_word
HAVING top_word <> ''
ORDER BY cnt DESC
LIMIT 10;
```
```
top_word	cnt
александр	119
the	106
of	93
сергей	89
&	77
алексей	68
dj	68
андрей	64
alexander	54
и	44
```