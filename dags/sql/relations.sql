ALTER TABLE memes
    ADD CONSTRAINT fk_memes_authors
    foreign key (author)
    REFERENCES authors (author_id);

ALTER TABLE memes
    ADD CONSTRAINT fk_memes_updaters
    foreign key (updater)
    REFERENCES updaters (updater_id);