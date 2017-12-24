import pandas as pd
import sys

filePath = '../data/lyrics.csv'

def main(num):
	df = pd.read_csv(filePath)
	df['word_count'] = df['lyrics'].str.split('\s').str.len()
	df_clean = df[df['word_count'] >= 100]
	df_clean = df_clean[df_clean['word_count'] <= 1000]
	del df_clean['index']
	df_clean = df_clean.reset_index(drop=True)
	if not num is None:
		df_clean = df_clean[:int(num)]
	df_clean.to_csv('../data/lyrics_cleaned.csv', header=True, index=True)

if __name__ == '__main__':
	main(sys.argv[1] if len(sys.argv) > 1 else None)
