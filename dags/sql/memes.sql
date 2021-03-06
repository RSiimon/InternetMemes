CREATE TABLE IF NOT EXISTS memes (
    meme_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR (255),
    url VARCHAR (255),
    description VARCHAR (1000),
    views INT,
    author VARCHAR(255),
    updater VARCHAR(255),
    added TIMESTAMP,
    updated TIMESTAMP,
    year INT,
    has_relations BOOLEAN,
    has_other_texts BOOLEAN,
    has_refs BOOLEAN,
    has_DBpedia BOOLEAN,
    status VARCHAR(255),
    adult VARCHAR(255),
    medical VARCHAR(255),
    racy VARCHAR(255),
    spoof VARCHAR(255),
    violence VARCHAR(255),
    template_image_url VARCHAR(255),
    fb_image_url VARCHAR(255),
    alt_img_urls VARCHAR(255)
);