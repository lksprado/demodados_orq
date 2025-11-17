# Como Rodar
Iniciar instância Airflow:
```shell 
astro dev start
```
Dependências
Pastas `./dbt` e `./include/local_setup` são submódulos. Após commit e push nos repos originais, para trazer códigos atualizados executar:
```shell
git pull origin main
git submodule update --remote
```

# Troubleshooting
Problema:
Erro ao criar tabela via dataframe com pandas `to_sql`: "Engine object has no attribute 'cursor' "
Solução:
Downgrade da versão do pandas para 2.1.4 para funcionar com sqlalchemy 1.4.54
