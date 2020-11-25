USE b_apteka;

/*SHOW TABLES;
SELECT * FROM categories c2 LIMIT 3;
SELECT * FROM cities c2 LIMIT 3;
SELECT * FROM manufacturers m2 LIMIT 3;
SELECT * FROM mnn m2 LIMIT 3;
SELECT * FROM order_statuses os LIMIT 3;
SELECT * FROM orders o2 LIMIT 3;
SELECT * FROM pharmacies p2 LIMIT 3;
SELECT * FROM products p2 LIMIT 3;
SELECT * FROM release_forms rf LIMIT 3;
SELECT * FROM stocks;
SELECT * FROM users u2 LIMIT 3;*/

-- Остатки аптеки г.Оренбург, пр-т Победы, д.119
SELECT
	ph.address AS 'Аптека',
	p.name AS 'Товар',
	mf.name AS 'Производитель',
	s.price AS 'Цена',
	s.value AS 'Количество',
	(s.price * s.value) AS 'Стоимость'
FROM products p
	JOIN stocks s
		ON p.id = s.products_id
	JOIN mnn
		ON p.mnn_id = mnn.id
	JOIN manufacturers mf
		ON mf.id = p.manufacturer_id
	JOIN pharmacies ph
		ON ph.id = s.pharmacies_id
	WHERE ph.address = 'г.Оренбург, пр-т Победы, д.119';
	
-- Наличие конкретного товара по аптекам
SELECT
	ph.address AS 'Аптека',
	p2.name AS 'Препарат',
	release_forms.name AS 'Лек.форма',
	m.name AS 'Производитель',
	s.value AS 'Кол-во'
FROM
	products p2
	JOIN stocks s ON
		p2.id = s.products_id
	JOIN pharmacies ph ON
		s.pharmacies_id = ph.id
	JOIN manufacturers m ON
		p2.manufacturer_id = m.id
	JOIN release_forms ON
		p2.release_forms_id = release_forms.id
WHERE
	p2.id = 16 AND p2.release_forms_id = 4;

-- заказ
SELECT * FROM orders o2 WHERE id = 2;
SELECT
	u.name AS 'Покупатель',
	o.created_at AS 'Дата заказа',
	o.id AS 'Номер заказа',
	pr.name AS 'Товар',
	m.name AS 'Производитель',
	o.number_products AS 'Количество'
FROM
	orders o
	JOIN users u ON
		o.user_id = u.id
	JOIN products pr ON
		pr.id = o.products_id
	JOIN manufacturers m ON
		m.id = pr.manufacturer_id
WHERE u.id = 3;


-- Представления

	-- №1 Лекарства в каталогах
CREATE VIEW drugs_in_cat AS
SELECT
	c.categories_name AS 'Категория',
	p.name AS 'Товар',
	m.name AS 'Производитель'
FROM
	categories c
JOIN products p ON
	c.products_id = p.id
JOIN manufacturers m ON
	p.manufacturer_id = m.id;
		
SELECT * FROM drugs_in_cat dic;


	-- №2 Заказы
CREATE VIEW all_orders AS
SELECT
	u.name AS 'Клиент',
	u.tel AS 'Тел',
	o2.id AS 'Номер заказа',
	o2.created_at,
	os.status AS 'Создан',
	(
	SELECT
		stocks.price
	FROM
		stocks
	WHERE
		products_id = o2.products_id
		AND stocks.pharmacies_id = o2.pharmacies_id) * o2.number_products AS 'Сумма заказа'
FROM
	orders o2
JOIN products p ON
	o2.products_id = p.id
JOIN pharmacies ph ON
	ph.id = o2.id
JOIN users u ON
	u.id = o2.user_id
JOIN order_statuses os ON
	os.id = o2.order_status_id;
	
SELECT * FROM all_orders ao;
