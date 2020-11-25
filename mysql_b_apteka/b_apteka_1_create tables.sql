DROP DATABASE IF EXISTS b_apteka;

-- создаем саму базу данных
CREATE DATABASE IF NOT EXISTS b_apteka;
USE b_apteka;

-- таблица населенных пунктов
CREATE TABLE IF NOT EXISTS cities ( 
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(150) NOT NULL UNIQUE)
COMMENT='города';

-- таблица аптек,
CREATE TABLE IF NOT EXISTS pharmacies (
	id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT NOT NULL,
    signboard VARCHAR(40) NOT NULL COMMENT 'Вывеска (бренд аптеки)',
    address VARCHAR(100) NOT NULL UNIQUE COMMENT 'Адрес',
    cities_id INT UNSIGNED COMMENT 'город',
    tel CHAR(11) COMMENT 'телефон',
    opening_hours VARCHAR(50) COMMENT 'часы работы',
    CONSTRAINT pharmacies_cities_id_fk FOREIGN KEY (cities_id) REFERENCES b_apteka.cities(id) ON DELETE CASCADE ON UPDATE CASCADE
    )
COMMENT='список аптек';

-- таблица МНН (международное неторговое наменование лекарства)
CREATE TABLE IF NOT EXISTS mnn (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(150) NOT NULL UNIQUE)
COMMENT='Международные неторговые наменования лекарств';

-- таблица лекарственных форм
CREATE TABLE IF NOT EXISTS release_forms (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(150) NOT NULL UNIQUE)
COMMENT='Лекарственные формы';

-- таблица производителей
CREATE TABLE IF NOT EXISTS manufacturers (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(150) NOT NULL UNIQUE COMMENT 'Название изготовителя',
	country VARCHAR(150) COMMENT 'Страна-производитель')
COMMENT='Производители';

-- таблица товаров
CREATE TABLE IF NOT EXISTS products (
	id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT NOT NULL,
	name VARCHAR(150) NOT NULL COMMENT 'Название препарата',
	release_forms_id INT UNSIGNED COMMENT 'Лек. форма',
	manufacturer_id INT UNSIGNED NOT NULL COMMENT ' Производитель',
	mnn_id INT UNSIGNED,
	barcode BIGINT(13) UNSIGNED UNIQUE,
CONSTRAINT products_release_forms_id_fk
	FOREIGN KEY (release_forms_id) REFERENCES b_apteka.release_forms(id)
	ON DELETE NO ACTION ON UPDATE CASCADE,
CONSTRAINT products_manufacturer_id_fk
	FOREIGN KEY (manufacturer_id) REFERENCES b_apteka.manufacturers(id)
	ON DELETE NO ACTION ON UPDATE CASCADE,
CONSTRAINT products_mnn_id_fk
	FOREIGN KEY (mnn_id) REFERENCES b_apteka.mnn(id)
	ON DELETE NO ACTION ON UPDATE CASCADE
) COMMENT='Товары';

-- таблица групп товаров
CREATE TABLE IF NOT EXISTS categories (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	categories_name VARCHAR(150) COMMENT 'Название группы',
	products_id INT UNSIGNED NOT NULL,
CONSTRAINT categories_products_id_fk
	FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
) COMMENT='Категории товаров';

	
-- таблица остатков по аптекам
CREATE TABLE IF NOT EXISTS stocks (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	products_id INT UNSIGNED NOT NULL,
	pharmacies_id INT UNSIGNED NOT NULL,
	price DECIMAL(11,2) COMMENT 'Цена',
	value INT(10) UNSIGNED COMMENT 'Запас товарной позиции в аптеке',
CONSTRAINT stocks_products_id_fk
	FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
	ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT stocks_pharmacies_id_fk
	FOREIGN KEY (pharmacies_id) REFERENCES b_apteka.pharmacies(id)
	ON DELETE RESTRICT ON UPDATE CASCADE
) COMMENT='Остатки по аптекам';

-- таблица покупателей
CREATE TABLE IF NOT EXISTS users (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY UNIQUE KEY,
	name VARCHAR(255) DEFAULT NULL COMMENT 'Имя покупателя',
	birthday_at DATE DEFAULT NULL COMMENT 'Дата рождения, может потребоваться для акций для именинников',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	tel CHAR(11) COMMENT 'телефон'	
) COMMENT='Покупатели';

-- таблица составов заказов
/*CREATE TABLE IF NOT EXISTS orders_products (
	id INT UNSIGNED UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	products_id INT UNSIGNED NOT NULL COMMENT 'id продукта',
	number_products INT UNSIGNED NOT NULL COMMENT 'количество товаров',
CONSTRAINT orders_products_products_id_fk
		FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
		ON DELETE NO ACTION ON UPDATE CASCADE
) COMMENT='Состав заказа';*/

-- таблица статусов заказов
CREATE TABLE order_statuses (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY UNIQUE KEY,
	status VARCHAR(22) DEFAULT NULL COMMENT 'Статус заказа'
) COMMENT='Статусы заказов';

-- таблица заказов
CREATE TABLE IF NOT EXISTS orders (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY UNIQUE,
	user_id INT UNSIGNED NOT NULL COMMENT 'Покупатель',
	products_id INT UNSIGNED NOT NULL COMMENT 'id продукта',
	number_products INT UNSIGNED NOT NULL COMMENT 'количество товаров',	
	order_status_id INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'Статус заказа',
	pharmacies_id INT UNSIGNED NOT NULL COMMENT 'Аптека, выдавшая заказ',
	created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
	updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
CONSTRAINT orders_user_id_fk
	FOREIGN KEY (user_id) REFERENCES b_apteka.users(id) 
	ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT orders_order_status_id_fk
	FOREIGN KEY (order_status_id) REFERENCES b_apteka.order_statuses(id)
	ON DELETE NO ACTION ON UPDATE CASCADE,
CONSTRAINT orders_pharmacies_id_fk
	FOREIGN KEY (pharmacies_id) REFERENCES b_apteka.pharmacies(id)
	ON DELETE NO ACTION ON UPDATE CASCADE,
CONSTRAINT orders_products_products_id_fk
		FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
		ON DELETE NO ACTION ON UPDATE CASCADE
) COMMENT='Заказы';


SHOW TABLES;
