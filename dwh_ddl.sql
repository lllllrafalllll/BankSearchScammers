
create table de11an.mrve_stg_transactions (
	transaction_id char(11),
	transaction_date timestamp(0),
	amount numeric(7, 2),
	card_num char(19),
	oper_type char(8),
	oper_result char(7),
	terminal char(7)
);



create table de11an.mrve_dwh_fact_transactions(
	transaction_id char(11),
	transaction_date timestamp(0),
	amount numeric(7, 2),
	card_num char(19),
	oper_type char(8),
	oper_result char(7),
	terminal char(7)
);


create table de11an.mrve_stg_terminals (
	terminal_id char(5),
	terminal_type char(3),
	terminal_city varchar(200),
	terminal_address varchar(200)
	create_dt timestamp(0),
);


create table de11an.mrve_stg_blacklist (
	date timestamp(0),
	passport char(11),
);


CREATE TABLE de11an.mrve_stg_accounts (
	account char(20),
	valid_to timestamp(0),
	client varchar(10),
	create_dt timestamp(0),
	update_dt timestamp(0)
);


CREATE TABLE de11an.mrve_dwh_dim_accounts_hist (
	account char(20),
	valid_to timestamp(0),
	client varchar(10),
    effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
);



CREATE TABLE de11an.mrve_stg_cards (
	card_num char(20),
	account char(20),
	create_dt timestamp(0),
	update_dt timestamp(0)
);


CREATE TABLE de11an.mrve_dwh_dim_cards_hist (
	card_num char(20),
	account char(20),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
);


CREATE TABLE de11an.mrve_stg_clients(
	client_id char(8),
	last_name char(30),
	first_name char(30),
	patronymic char(14),
	date_of_birth timestamp(0),
	passport_num char(11),
	passport_valid_to timestamp(0),
	phone char(16),
	create_dt timestamp(0),
	update_dt timestamp(0)
	);


CREATE TABLE de11an.mrve_dwh_dim_clients_hist(
	client_id char(8),
	last_name char(30),
	first_name char(30),
	patronymic char(14),
	date_of_birth timestamp(0),
	passport_num char(11),
	passport_valid_to timestamp(0),
	phone char(16),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
	)


create table de11an.mrve_dwh_dim_terminals_hist (
	terminal_id char(5),
	terminal_type char(3),
	terminal_city varchar(200),
	terminal_address varchar(200),
	effective_from timestamp(0), 
	effective_to timestamp(0), 
	deleted_flg integer
);



create table de11an.mrve_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);


create table de11an.mrve_stg_del( 
	id varchar(30)
);

create table de11an.mrve_dwh_fact_passport_blacklist(
	enty_dt timestamp(0),
	passport_num varchar(11),
);


create table de11an.mrve_rep_fraud(
	event_dt timestamp(0),
	passport varchar(11),
	fio varchar(30),
	phone char(16),
	event_type integer,
	report_dt timestamp(0),
)

insert into de11an.mrve_meta( schema_name, table_name, max_update_dt )
values( 'de11an','mrve_stg_terminals', to_timestamp('1900-01-01','YYYY-MM-DD'));
insert into de11an.mrve_meta( schema_name, table_name, max_update_dt )
values( 'de11an','mrve_stg_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD'));
insert into de11an.mrve_meta( schema_name, table_name, max_update_dt )
values( 'de11an','mrve_stg_clients', to_timestamp('1899-01-01','YYYY-MM-DD'));
insert into de11an.mrve_meta( schema_name, table_name, max_update_dt )
values( 'de11an','mrve_stg_cards', to_timestamp('1899-01-01','YYYY-MM-DD'));
insert into de11an.mrve_meta( schema_name, table_name, max_update_dt )
values( 'de11an','mrve_stg_accounts', to_timestamp('1899-01-01','YYYY-MM-DD'));

