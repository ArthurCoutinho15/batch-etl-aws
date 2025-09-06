import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

class FinanceDataGenerator:
    def __init__(self):
        self.fake = Faker('pt_BR')
        random.seed(42)  # Para reproduzibilidade
        np.random.seed(42)
    
    def generate_account_ids(self, num_accounts=50):
        """Gera IDs de contas no formato ACC0001, ACC0002, etc."""
        return [f"ACC{str(i).zfill(4)}" for i in range(1, num_accounts + 1)]
    
    def generate_client_positions(self, 
                                start_date='2024-01-01', 
                                end_date='2024-12-31', 
                                num_accounts=50):
        """
        Gera dados fake de posições diárias dos clientes
        """
        # Gerar lista de contas
        accounts = self.generate_account_ids(num_accounts)
        
        # Gerar range de datas
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        date_range = pd.date_range(start=start, end=end, freq='D')
        
        data = []
        
        for account in accounts:
            # Posição inicial aleatória para cada conta (entre 10k e 10M)
            initial_position = random.uniform(10_000, 10_000_000)
            current_position = initial_position
            
            # Perfil de volatilidade para cada conta
            volatility = random.uniform(0.001, 0.02)  # 0.1% a 2% de volatilidade diária
            trend = random.uniform(-0.0005, 0.0015)  # Tendência diária (-0.05% a +0.15%)
            
            for date in date_range:
                # Simula mudanças diárias com tendência e volatilidade
                daily_change = np.random.normal(trend, volatility)
                current_position *= (1 + daily_change)
                
                # Adiciona alguns eventos especiais (grandes mudanças ocasionais)
                if random.random() < 0.02:  # 2% chance de evento especial
                    shock = random.uniform(-0.1, 0.15)  # -10% a +15%
                    current_position *= (1 + shock)
                
                # Garante que a posição não fique negativa
                current_position = max(current_position, 1000)
                
                data.append({
                    'conta': account,
                    'data': date.strftime('%Y-%m-%d'),
                    'posicao_usd': round(current_position, 2),
                    'data_timestamp': date
                })
        
        df = pd.DataFrame(data)
        return df
    
    def add_client_info(self, df):
        """Adiciona informações dos clientes"""
        accounts = df['conta'].unique()
        client_info = []
        
        for account in accounts:
            client_info.append({
                'conta': account,
                'nome_cliente': self.fake.name(),
                'email': self.fake.email(),
                'cidade': self.fake.city(),
                'estado': self.fake.state(),
                'data_abertura': self.fake.date_between(start_date='-5y', end_date='-1y'),
                'tipo_conta': random.choice(['Premium', 'Standard', 'VIP']),
                'gestor_responsavel': self.fake.name()
            })
        
        client_df = pd.DataFrame(client_info)
        return df.merge(client_df, on='conta', how='left')
    
    def calculate_metrics(self, df):
        """Calcula métricas financeiras"""
        df = df.sort_values(['conta', 'data_timestamp'])
        
        # Calcula retorno diário
        df['retorno_diario'] = df.groupby('conta')['posicao_usd'].pct_change()
        
        # Calcula retorno acumulado desde o início do ano
        df['retorno_ytd'] = df.groupby('conta')['posicao_usd'].apply(
            lambda x: (x / x.iloc[0] - 1) * 100
        ).reset_index(level=0, drop=True)
        
        # Calcula média móvel de 30 dias
        df['media_movel_30d'] = df.groupby('conta')['posicao_usd'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        
        # Calcula volatilidade de 30 dias
        df['volatilidade_30d'] = df.groupby('conta')['retorno_diario'].transform(
            lambda x: x.rolling(window=30, min_periods=1).std() * np.sqrt(252) * 100
        )
        
        return df
    
    def generate_complete_dataset(self, 
                                start_date='2024-01-01', 
                                end_date='2024-12-31', 
                                num_accounts=50):
        """Gera dataset completo com todas as informações"""
        print("Gerando posições diárias...")
        df = self.generate_client_positions(start_date, end_date, num_accounts)
        
        print("Adicionando informações dos clientes...")
        df = self.add_client_info(df)
        
        print("Calculando métricas financeiras...")
        df = self.calculate_metrics(df)
        
        print(f"Dataset gerado com sucesso! {len(df)} registros criados.")
        return df

# Exemplo de uso
if __name__ == "__main__":
    generator = FinanceDataGenerator()
    
    # Gera dataset completo
    df_positions = generator.generate_complete_dataset(
        start_date='2024-01-01',
        end_date='2024-12-31',
        num_accounts=50
    )
    
    # Mostra informações básicas
    print(f"\nShape do dataset: {df_positions.shape}")
    print(f"\nColunas: {list(df_positions.columns)}")
    print(f"\nPrimeiras linhas:")
    print(df_positions.head())
    
    # Estatísticas básicas
    print(f"\nEstatísticas das posições:")
    print(df_positions['posicao_usd'].describe())
    
    # Salva em CSV para teste
    df_positions.to_csv('posicoes_clientes_fake.csv', index=False)
    print(f"\nArquivo salvo como 'posicoes_clientes_fake.csv'")