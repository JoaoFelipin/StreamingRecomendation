import pandas as pd 


class melhores_filmes():
    def __init__(self,ratings,movies):
        self.ratings = ratings
        self.movies = movies
        return
    
    def get_top_N(self,N):
        recomendation =(
            self.ratings.groupby('item_id')['user_id'].count()
            .reset_index()
            .rename({'user_id':'score'},axis=1)
            .sort_values(by='score',ascending=False)
        )
        recomendation= recomendation.merge(self.movies,on='item_id',how='inner')

        return recomendation.head(N)
    
    def get_top_n_eval():

        return


df_ratings = pd.read_parquet(r'D:\Desktop\Scripts\StreamingRecomendation\data\ratings.parquet')
df_movies = pd.read_parquet(r'D:\Desktop\Scripts\StreamingRecomendation\data\movies.parquet')

recomend = melhores_filmes(ratings=df_ratings,movies=df_movies)
print(recomend.get_top_N(3))