
#Запросы для создание таблиц форматом SCD2
class Scd2:
    def __init__(self) -> None:
        pass

    def terminals_loading(self):
        request = ''' insert into de11an.mrve_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, 
                terminal_address, effective_from, effective_to, deleted_flg )
                select 
                    stg.terminal_id, 
                    stg.terminal_type, 
                    stg.terminal_city, 
                    stg.terminal_address, 
                    stg.create_dt,
                    to_date( '9999-12-31', 'YYYY-MM-DD' ),
                    0
                from de11an.mrve_stg_terminals stg
                left join de11an.mrve_dwh_dim_terminals_hist tgt
                on stg.terminal_id = tgt.terminal_id
                    and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and tgt.deleted_flg = 0
                where tgt.terminal_id is null; '''
        
        print('Загрузка в приемник "вставок" на источнике (формат SCD2).')
        return request
    
    def terminals_update(self):
        request = ''' update de11an.mrve_dwh_dim_terminals_hist
                    set 
                        effective_to = tmp.create_dt - interval '1 second'
                    from (
                        select 
                            stg.terminal_id, 
                            stg.create_dt,
                            tgt.effective_to
                        from de11an.mrve_stg_terminals stg
                        inner join de11an.mrve_dwh_dim_terminals_hist tgt
                        on stg.terminal_id = tgt.terminal_id
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                        where 1=0
                        or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
                        or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
                        or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )
                    ) tmp
                    where de11an.mrve_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id and de11an.mrve_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 


                    insert into de11an.mrve_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, 
                    terminal_address, effective_from, effective_to, deleted_flg )
                    select 
                        stg.terminal_id, 
                        stg.terminal_type, 
                        stg.terminal_city, 
                        stg.terminal_address, 
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from de11an.mrve_stg_terminals stg
                    inner join de11an.mrve_dwh_dim_terminals_hist tgt
                    on stg.terminal_id = tgt.terminal_id
                        and tgt.effective_to = stg.create_dt - interval '1 second'
                        and tgt.deleted_flg = 0
                    where 1 = 0
                        or stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )
                        or stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null )
                        or stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null )'''
        
        print('Обновление в приемнике "обновлений" на источнике (формат SCD2).')
        return request
    

    def terminals_delete(self):
        request = '''
        insert into de11an.mrve_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, 
        terminal_address, effective_from, effective_to, deleted_flg )
        select 
            tgt.terminal_id, 
            tgt.terminal_type, 
            tgt.terminal_city, 
            tgt.terminal_address, 
            Now(),
            to_date( '9999-12-31', 'YYYY-MM-DD' ),
            1
        from de11an.mrve_dwh_dim_terminals_hist tgt
        where tgt.terminal_id in (
            select tgt.terminal_id
            from de11an.mrve_dwh_dim_terminals_hist tgt
            left join de11an.mrve_stg_del stg
            on stg.id = tgt.terminal_id
            where stg.id is null
                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and tgt.deleted_flg = 0
        )
        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and tgt.deleted_flg = 0;



        update de11an.mrve_dwh_dim_terminals_hist
        set 
            effective_to = now() - interval '1 second'
        where de11an.mrve_dwh_dim_terminals_hist.terminal_id in (
            select tgt.terminal_id
            from de11an.mrve_dwh_dim_terminals_hist tgt
            left join de11an.mrve_stg_del stg
            on stg.id = tgt.terminal_id
            where stg.id is null
                and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                and tgt.deleted_flg = 0
        )
        and de11an.mrve_dwh_dim_terminals_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
        and de11an.mrve_dwh_dim_terminals_hist.deleted_flg = 0;
                '''
        
        print('Удаление в приемнике удаленных в источнике записей (формат SCD2).')
        return request
    

    def accounts_loading(self):
       request = '''insert into de11an.mrve_dwh_dim_accounts_hist(account, valid_to, client, effective_from, effective_to, deleted_flg)
                    select 
                        stg.account,
                        stg.valid_to,
                        stg.client,
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from  de11an.mrve_stg_accounts stg
                    left join de11an.mrve_dwh_dim_accounts_hist tgt
                    on stg.account = tgt.account
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 0
                    where tgt.account is null; ''' 
       
       print('Загрузка в приемник "вставок" на источнике (формат SCD2).')
       return request
    
    def accounts_update(self):
        request = '''update de11an.mrve_dwh_dim_accounts_hist
                    set 
                        effective_to = tmp.create_dt - interval '1 second'
                    from (
                        select 
                            stg.account, 
                            stg.create_dt
                        from de11an.mrve_stg_accounts stg
                        inner join de11an.mrve_dwh_dim_accounts_hist tgt
                        on stg.account = tgt.account
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                        where 1=0
                            or stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )
                            or stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null )
                    ) tmp
                    where de11an.mrve_dwh_dim_accounts_hist.account = tmp.account and de11an.mrve_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); ; 



                insert into de11an.mrve_dwh_dim_accounts_hist(account, valid_to, client, effective_from, effective_to, deleted_flg)
                select 
                    stg.account,
                    stg.valid_to,
                    stg.client,
                    stg.create_dt,
                    to_date( '9999-12-31', 'YYYY-MM-DD' ),
                    0
                from de11an.mrve_stg_accounts stg
                inner join de11an.mrve_dwh_dim_accounts_hist tgt
                on stg.account = tgt.account
                    and tgt.effective_to = stg.create_dt - interval '1 second'
                    and tgt.deleted_flg = 0
                where 1 = 0
                        or stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null )
                        or stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null ) '''
        
        print('Обновление в приемнике "обновлений" на источнике (формат SCD2).')
        return request
    
    def accounts_delete(self):
        request = ''' insert into de11an.mrve_dwh_dim_accounts_hist(account, valid_to, client, effective_from, effective_to, deleted_flg)
                    select 
                        tgt.account,
                        tgt.valid_to,
                        tgt.client,
                        Now(),
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        1
                    from de11an.mrve_dwh_dim_accounts_hist tgt
                    where tgt.account in (
                        select tgt.account
                        from de11an.mrve_dwh_dim_accounts_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.account
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and tgt.deleted_flg = 0;


                    update de11an.mrve_dwh_dim_accounts_hist
                    set 
                        effective_to = now() - interval '1 second'
                    where de11an.mrve_dwh_dim_accounts_hist.account in (
                        select tgt.account
                        from de11an.mrve_dwh_dim_cards_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.account
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and de11an.mrve_dwh_dim_accounts_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and de11an.mrve_dwh_dim_accounts_hist.deleted_flg = 0;'''
        
        print('Удаление в приемнике удаленных в источнике записей (формат SCD2).')
        return request
    
    def cards_loading(self):
        request = '''insert into de11an.mrve_dwh_dim_cards_hist(card_num, account, effective_from, effective_to, deleted_flg)
                    select 
                        stg.card_num,
                        stg.account,
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from de11an.mrve_stg_cards stg
                    left join de11an.mrve_dwh_dim_cards_hist tgt
                    on stg.account = tgt.account
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 0
                    where tgt.account is null;'''
        
        print('Загрузка в приемник "вставок" на источнике (формат SCD2).')
        return request

    def cards_update(self):
        request = ''' update de11an.mrve_dwh_dim_cards_hist
                    set 
                        effective_to = tmp.create_dt - interval '1 second'
                    from (
                        select 
                            stg.account, 
                            stg.create_dt
                        from de11an.mrve_stg_cards stg
                        inner join de11an.mrve_dwh_dim_cards_hist tgt
                        on stg.account = tgt.account
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                        where 1=0
                            or stg.card_num <> tgt.card_num or ( stg.card_num is null and tgt.card_num is not null ) or ( stg.card_num is not null and tgt.card_num is null )
                    ) tmp
                    where de11an.mrve_dwh_dim_cards_hist.account = tmp.account and de11an.mrve_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); ; 



                    insert into de11an.mrve_dwh_dim_cards_hist(card_num, account, effective_from, effective_to, deleted_flg)
                    select 
                        stg.card_num,
                        stg.account,
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from de11an.mrve_stg_cards stg
                    inner join de11an.mrve_dwh_dim_cards_hist tgt
                    on stg.account = tgt.account
                        and tgt.effective_to = stg.create_dt - interval '1 second'
                        and tgt.deleted_flg = 0
                    where 1 = 0
                            or stg.card_num <> tgt.card_num or ( stg.card_num is null and tgt.card_num is not null ) or ( stg.card_num is not null and tgt.card_num is null )'''
        
        print('Обновление в приемнике "обновлений" на источнике (формат SCD2).')
        return request

    def cards_delete(self):
        request = '''insert into de11an.mrve_dwh_dim_cards_hist(card_num, account, effective_from, effective_to, deleted_flg)
                    select 
                        tgt.card_num,
                        tgt.account,
                        Now(),
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        1
                    from de11an.mrve_dwh_dim_cards_hist tgt
                    where tgt.account in (
                        select tgt.account
                        from de11an.mrve_dwh_dim_cards_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.account
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and tgt.deleted_flg = 0;



                    update de11an.mrve_dwh_dim_cards_hist
                    set 
                        effective_to = now() - interval '1 second'
                    where de11an.mrve_dwh_dim_cards_hist.account in (
                        select tgt.account
                        from de11an.mrve_dwh_dim_cards_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.account
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and de11an.mrve_dwh_dim_cards_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and de11an.mrve_dwh_dim_cards_hist.deleted_flg = 0;
                            
                            '''
        print('Удаление в приемнике удаленных в источнике записей (формат SCD2).')
        return request

    def clients_loading(self):
        request = '''insert into de11an.mrve_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, 
                    passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    select 
                        stg.client_id,
                        stg.last_name,
                        stg.first_name,
                        stg.patronymic,
                        stg.date_of_birth,
                        stg.passport_num,
                        stg.passport_valid_to,
                        stg.phone,
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from de11an.mrve_stg_clients stg
                    left join de11an.mrve_dwh_dim_clients_hist tgt
                    on stg.client_id = tgt.client_id
                        and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                        and tgt.deleted_flg = 0
                    where tgt.client_id is null;
                            '''
        
        print('Загрузка в приемник "вставок" на источнике (формат SCD2).')
        return request 
    
    def clients_update(self):
        request = '''update de11an.mrve_dwh_dim_clients_hist
                    set 
                        effective_to = tmp.create_dt - interval '1 second'
                    from (
                        select 
                            stg.client_id, 
                            stg.create_dt
                        from de11an.mrve_stg_clients stg
                        inner join de11an.mrve_dwh_dim_clients_hist tgt
                        on stg.client_id = tgt.client_id
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                        where 1=0
                            or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )
                            or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null)
                            or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
                            or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
                            or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
                            or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null )

                    ) tmp
                    where de11an.mrve_dwh_dim_clients_hist.client_id = tmp.client_id and de11an.mrve_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ); 



                    insert into de11an.mrve_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, 
                    passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    select 
                        stg.client_id,
                        stg.last_name,
                        stg.first_name,
                        stg.patronymic,
                        stg.date_of_birth,
                        stg.passport_num,
                        stg.passport_valid_to,
                        stg.phone,
                        stg.create_dt,
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        0
                    from de11an.mrve_stg_clients stg
                    inner join de11an.mrve_dwh_dim_clients_hist tgt
                    on stg.client_id = tgt.client_id
                        and tgt.effective_to = stg.create_dt - interval '1 second'
                        and tgt.deleted_flg = 0
                    where 1 = 0
                        or stg.last_name <> tgt.last_name or ( stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null )
                        or stg.patronymic <> tgt.patronymic or ( stg.patronymic is null and tgt.patronymic is not null ) or ( stg.patronymic is not null and tgt.patronymic is null)
                        or stg.first_name <> tgt.first_name or ( stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null )
                        or stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null )
                        or stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null )
                        or stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null )'''
        
        print('Обновление в приемнике "обновлений" на источнике (формат SCD2).')
        return request

    def clients_delete(self):
        request = '''insert into de11an.mrve_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, 
                    passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    select 
                        tgt.client_id,
                        tgt.last_name,
                        tgt.first_name,
                        tgt.patronymic,
                        tgt.date_of_birth,
                        tgt.passport_num,
                        tgt.passport_valid_to,
                        tgt.phone,
                        Now(),
                        to_date( '9999-12-31', 'YYYY-MM-DD' ),
                        1
                    from de11an.mrve_dwh_dim_clients_hist tgt
                    where tgt.client_id in (
                        select tgt.client_id
                        from de11an.mrve_dwh_dim_clients_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.client_id
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and tgt.deleted_flg = 0;



                    update de11an.mrve_dwh_dim_clients_hist
                    set 
                        effective_to = now() - interval '1 second'
                    where de11an.mrve_dwh_dim_clients_hist.client_id in (
                        select tgt.client_id
                        from de11an.mrve_dwh_dim_clients_hist tgt
                        left join de11an.mrve_stg_del stg
                        on stg.id = tgt.client_id
                        where stg.id is null
                            and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                            and tgt.deleted_flg = 0
                    )
                    and de11an.mrve_dwh_dim_clients_hist.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
                    and de11an.mrve_dwh_dim_clients_hist.deleted_flg = 0;'''
        
        print('Удаление в приемнике удаленных в источнике записей (формат SCD2).')
        return request
