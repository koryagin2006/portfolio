USE b_apteka;
SHOW TABLES;

CREATE UNIQUE INDEX mnn_name_idx ON mnn(name);

CREATE INDEX orders_user_id_idx ON orders(user_id);

CREATE INDEX products_name_idx ON products(name);

CREATE INDEX  users_name_idx ON users(name);

CREATE UNIQUE INDEX categories_categories_name_idx ON categories(categories_name);