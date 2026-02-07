# Repo Teste Drift

Este repo é um conjunto simples de arquivos para testar:
- Onboarding em lote
- Drift em PR
- Suporte a .py e .ipynb

## Estrutura
- `pipelines/`: scripts Python com ETL simplificado
- `notebooks/`: notebooks com código de exemplo
- `configs/`: configs fictícias

## Como testar
1. Suba este repo no GitHub.
2. Rode `/git/import` para gerar baselines.
3. Crie um PR alterando um arquivo em `pipelines/`.
4. Rode `/git/pr/analyze`.
