

 #Функция для поиска признаков мошенничества
class Report:
    def __init__(self) -> None:
        pass

   
    def report_one(self):
        request = '''insert into de11an.mrve_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
                    WITH blist as (
                    select distinct clients.passport_num as passport_num
                    from de11an.mrve_dwh_dim_clients_hist clients
                    inner join de11an.mrve_dwh_fact_passport_blacklist blacklist
                    on clients.passport_num = blacklist.passport_num)

                    select oper_one.transaction_date, oper_one.passport_num, oper_one.fio, oper_one.phone, oper_one.event_type, oper_one.report_dt
                    from (
                    select a.transaction_date,  b.passport_num, (b.first_name || b.last_name) as fio, b.phone, 1 as event_type, now() as report_dt 
                    from de11an.mrve_dwh_fact_transactions as a
                    inner join (
                    select distinct client_id, passport_valid_to, last_name, first_name, phone, passport_num, accounts.account, cards.card_num
                    from de11an.mrve_dwh_dim_clients_hist clients
                    left join de11an.mrve_dwh_dim_accounts_hist accounts on
                    clients.client_id  = accounts.client
                    left join de11an.mrve_dwh_dim_cards_hist cards on
                    accounts.account  = cards.account
                    where  passport_valid_to  < now() ) as b
                    on a.card_num = b.card_num
                    where a.transaction_date > b.passport_valid_to or passport_num in (select passport_num from blist)) oper_one
                    left join de11an.mrve_rep_fraud rep
                    on oper_one.transaction_date = rep.event_dt
                    and oper_one.passport_num = rep.passport
                    and oper_one.fio = rep.fio
                    and oper_one.phone = rep.phone
                    where rep.passport is null'''
        print('Признаки мошеннических операций 1')
        return request
    

    def report_two(self):
        request = '''insert into de11an.mrve_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
                    select oper_two.transaction_date, oper_two.passport_num, oper_two.fio, oper_two.phone, oper_two.event_type, oper_two.report_dt 
                    from (
                    select  a.transaction_date,  b.passport_num, (b.first_name || b.last_name) as fio, b.phone, 2 as event_type, now() as report_dt
                    from de11an.mrve_dwh_fact_transactions a
                    inner join (select distinct client_id, passport_valid_to, last_name, first_name, phone, passport_num, accounts.account, cards.card_num, accounts.valid_to 
                    from de11an.mrve_dwh_dim_clients_hist clients
                    left join de11an.mrve_dwh_dim_accounts_hist accounts on
                    clients.client_id  = accounts.client
                    left join de11an.mrve_dwh_dim_cards_hist cards on
                    accounts.account  = cards.account) b 
                    on a.card_num = b.card_num
                    where a.transaction_date > b.valid_to) oper_two
                    left join de11an.mrve_rep_fraud rep
                    on oper_two.transaction_date = rep.event_dt
                    and oper_two.passport_num = rep.passport
                    and oper_two.fio = rep.fio
                    and oper_two.phone = rep.phone
                    where rep.passport is null'''

        print('Признаки мошеннических операций 2')
        return request
    


    def report_three(self):
        request = '''insert into de11an.mrve_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)                 
                    select distinct tr.transaction_date, client.passport_num, 
                    client.first_name || client.last_name, client.phone, 3, now()
                    from (
                    select ft.transaction_date, ft.card_num, dt.terminal_city,
                    ft.transaction_date - (lag(ft.transaction_date, 1) over w) as dt_delta,
                    (lag(dt.terminal_city, 1) over w) as term_prev, (lag(ft.card_num, 1) over w) as card_prev,
                    case 
                        when (lag(dt.terminal_city, 1) over w) != dt.terminal_city 
                        and  (lag(ft.card_num, 1) over w) = ft.card_num 
                        and  (ft.transaction_date - (lag(ft.transaction_date, 1) over w)) < '1 hour' then 1
                        else 0
                    end status
                    from de11an.mrve_dwh_fact_transactions ft
                    left join de11an.mrve_dwh_dim_terminals_hist dt
                    on ft.terminal = dt.terminal_id
                    window w as (partition by card_num order by card_num, transaction_date, terminal_city)
                    order by card_num, transaction_date, terminal_city) tr
                    left join de11an.mrve_dwh_dim_cards_hist cards
                    on tr.card_num = cards.card_num
                    left join de11an.mrve_dwh_dim_accounts_hist accounts
                    on cards.account = accounts.account 
                    left join de11an.mrve_dwh_dim_clients_hist client
                    on accounts.client = client.client_id
                    where status = 1'''

        print('Признаки мошеннических операций 3')
        return request
    

    def report_four(self):
        request = '''insert into de11an.mrve_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
                    select distinct tr.transaction_date, client.passport_num, 
                                        client.first_name ||' ' || client.last_name, client.phone, 4, now() from (
                    select *, (sum(status) over w) - status as total
                    from
                        ( select *,
                            case
                                when oper_result = 'REJECT'
                                and coalesce(amount - (lag(amount, 1) over w), -1) < 0 then 1
                                else 0
                            end status
                        from de11an.mrve_dwh_fact_transactions
                        where oper_type = 'WITHDRAW'
                            window w as (partition by card_num
                        order by card_num, transaction_date asc)
                        order by card_num, transaction_date asc) as a
                        window w as ( partition by card_num order by card_num, transaction_date
                                        rows between 3 preceding and current row)) tr
                        left join de11an.mrve_dwh_dim_cards_hist cards
                        on tr.card_num = cards.card_num
                        left join de11an.mrve_dwh_dim_accounts_hist accounts
                        on cards.account = accounts.account 
                        left join de11an.mrve_dwh_dim_clients_hist client
                        on accounts.client = client.client_id
                        where total > 2'''

        print('Признаки мошеннических операций 4')
        return request
    

    