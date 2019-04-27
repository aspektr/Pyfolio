from reader import Reader


if __name__ == '__main__':
    # t = Loader()
    # t.show('market_name')
    t = Reader(emitent_name=['SandP-500*', 'Золото', 'Brent'])\
        .read(reference={'emitent_name': 'SandP-500*'})
