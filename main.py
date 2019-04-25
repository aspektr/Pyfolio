from loader import Loader
from writer import Writer


if __name__ == '__main__':
    # t = Loader()
    # t.show('market_name')
    t = Writer(emitent_name=['Brent', 'Золото']).save()
