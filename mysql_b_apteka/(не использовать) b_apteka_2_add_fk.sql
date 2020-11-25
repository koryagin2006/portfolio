-- Создание внешних ключей
-- SHOW COLUMNS FROM pharmacies WHERE FIELD RLIKE '.*id';

-- внешний ключ на pharmacies
/*ALTER TABLE b_apteka.pharmacies 
	ADD CONSTRAINT pharmacies_cities_id_fk
		FOREIGN KEY (cities_id) REFERENCES b_apteka.cities(id) 
		ON DELETE CASCADE ON UPDATE CASCADE;*/

-- внешний ключ на orders
/*ALTER TABLE b_apteka.orders 
	ADD CONSTRAINT orders_user_id_fk
		FOREIGN KEY (user_id) REFERENCES b_apteka.users(id) 
		ON DELETE CASCADE ON UPDATE CASCADE,
	ADD CONSTRAINT orders_order_status_id_fk
		FOREIGN KEY (order_status_id) REFERENCES b_apteka.order_statuses(id)
		ON DELETE NO ACTION ON UPDATE CASCADE,
	ADD CONSTRAINT orders_orders_products_id_fk
		FOREIGN KEY (orders_products_id) REFERENCES b_apteka.orders_products(id)
		ON DELETE NO ACTION ON UPDATE NO ACTION,
	ADD CONSTRAINT orders_pharmacies_id_fk
		FOREIGN KEY (pharmacies_id) REFERENCES b_apteka.pharmacies(id)
		ON DELETE NO ACTION ON UPDATE CASCADE;*/

-- внешний ключ на orders_products
/*ALTER TABLE b_apteka.orders_products 
	ADD CONSTRAINT orders_products_products_id_fk
		FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
		ON DELETE NO ACTION ON UPDATE CASCADE;*/

-- внешний ключ на products
/*ALTER TABLE b_apteka.products 
	ADD CONSTRAINT products_release_forms_id_fk
		FOREIGN KEY (release_forms_id) REFERENCES b_apteka.release_forms(id)
		ON DELETE NO ACTION ON UPDATE CASCADE,
	ADD CONSTRAINT products_manufacturer_id_fk
		FOREIGN KEY (manufacturer_id) REFERENCES b_apteka.manufacturer(id)
		ON DELETE NO ACTION ON UPDATE CASCADE,
	ADD CONSTRAINT products_mnn_id_fk
		FOREIGN KEY (mnn_id) REFERENCES b_apteka.mnn(id)
		ON DELETE NO ACTION ON UPDATE CASCADE;*/

-- внешний ключ на stocks
/*ALTER TABLE b_apteka.stocks 
	ADD CONSTRAINT stocks_products_id_fk
		FOREIGN KEY (products_id) REFERENCES b_apteka.products(id)
		ON DELETE CASCADE ON UPDATE CASCADE,
	ADD CONSTRAINT stocks_pharmacies_id_fk
		FOREIGN KEY (pharmacies_id) REFERENCES b_apteka.pharmacies(id)
		ON DELETE RESTRICT ON UPDATE CASCADE;*/