from prototypes import Base
from reader import Reader
from datetime import datetime, timedelta
import os
import numpy as np
import scipy.optimize as spo
from utilites import normalize_data
from utilites import compute_daily_returns


class Portfolio(Base):
    def __init__(self, config, optimize=True):
        Base.__init__(self, config, path="config/portfolios/")

        self.market_id = self._get_list_sec('market_id')
        self.market_name = self._get_list_sec('market_name')
        self.emitent_id = self._get_list_sec('emitent_id')
        self.emitent_code = self._get_list_sec('emitent_code')
        self.emitent_name = self._get_list_sec('emitent_name')
        self.start_date = self._get_start_date()
        self.end_date = self._get_end_date()
        self.data = self._get_data()
        self.prices = self.data[[col for col in self.data.columns if '_Ref' not in col]]
        self.price_ref = self.data[[col for col in self.data.columns if '_Ref' in col]]
        self.error_func = self._minimize_function

        self.logger.info("[%u] Portfolio '%s' is ready:" % (os.getpid(), self.config['name']))
        print('Start date: ', self.start_date)
        print('End date: ', self.end_date)
        print('Symbols: ', list(self.prices.columns))

        if optimize:
            self.allocs = self._optimize()
            print("Allocations:", self.allocs*100)
            cr, adr, sddr, sr = self.get_portfolio_statistics(self.daily_portfolio_values(self.allocs))
            print("Sharpe Ratio:", sr)
            print("Volatility (stdev of daily returns):", sddr)
            print("Average Daily Return:", adr)
            print("Cumulative Return:", cr)

    def get_portfolio_statistics(self, port_val):
        port_daily_ret = self.compute_daily_returns(port_val)
        #for i in port_daily_ret.values:
        #    print(i)
        port_daily_ret = port_daily_ret[1:]

        cum_ret = port_val[-1] / port_val[0] - 1
        avg_daily_ret = port_daily_ret.mean()
        std_daily_ret = port_daily_ret.std()
        sharp_ratio = self.get_sharp_ratio(port_daily_ret)

        return cum_ret, avg_daily_ret, std_daily_ret, sharp_ratio

    def daily_portfolio_values(self, allocs):
        if np.any(self.prices.values[0, :] == 0):
            self.logger.error("[%u] Normed prices are needed instead of daily return" %
                              os.getpid())
            print(self.prices.head(3))
            raise SystemExit(1)
        elif np.all(self.prices.values[0, :] != 1):
            normed = normalize_data(self.prices)
        else:
            normed = self.prices.copy()

        for i, alloc in enumerate(allocs):
            if alloc < 0:
                normed.values[:, i] = normed.values[:, i] - 2

        alloced = normed * allocs
        pos_val = alloced * self.config['start_value']
        port_val = pos_val.sum(axis=1)
        return port_val

    @staticmethod
    def compute_daily_returns(df):
        """Compute and return the daily return values."""
        return compute_daily_returns(df)

    def get_sharp_ratio(self, port_ret, samples_per_year=252):
        """
            Calculate Sharp ratio
        :param port_ret: porfolio return (daily, weekly or monthly)
        :param samples_per_year: daily=252, weekly=52, monthly=12
        :return: annualized Sharp ratio
        """
        risk_free_rate = self.config['risk_free_rate']
        k = np.sqrt(samples_per_year)
        period_risk_free_rate = (risk_free_rate + 1) ** (1 / samples_per_year) - 1
        s = (port_ret - period_risk_free_rate).mean() / (port_ret - period_risk_free_rate).std()
        s_annualized = s * k
        return s_annualized

    def _minimize_function(self, allocs):
        port_val = self.daily_portfolio_values(allocs)
        port_daily_ret = self.compute_daily_returns(port_val)
        sharp_ratio = self.get_sharp_ratio(port_daily_ret)
        return sharp_ratio * -1

    def _optimize(self):

        def constraint(x):
            return np.abs(x).sum() - 1

        bounds=[]
        key = list(self.config['securities'].keys())[0]
        for sec in self.config['securities'][key]:
            if sec != self.config['reference'][key]:
                if self.config['securities'][key][sec]['short']:
                    bounds.append((-1, 1))
                else:
                    bounds.append((0, 1))

        same_avg = 1.0 / len(self.prices.columns)
        initial_guess = np.full((len(self.prices.columns)), -same_avg)
        cons = ({'type': 'eq', 'fun': constraint})
        #bounds = ((0, 1),) * len(self.prices.columns)
        result = spo.minimize(self.error_func,
                              initial_guess,
                              # args=(self.prices, self.config['start_value']),
                              method='SLSQP',
                              options={'disp': True},
                              constraints=cons,
                              bounds=bounds)
        return result.x

    def _get_end_date(self):
        if self.config['end_date'] is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
            return end_date

    def _get_start_date(self):
        if 'period' in self.config:
            shift = [int(s) for s in self.config['period'].split() if s.isdigit()][0]
            shift_date = (datetime.today() - timedelta(shift)).strftime('%Y-%m-%d')
            return shift_date

    def _get_list_sec(self, symbols_type):
            if symbols_type in self.config['securities']:
                return self.config['securities'][symbols_type]
            return {}

    def _get_data(self):
        return Reader(market_id=list(self.market_id.keys()),
                      market_name=list(self.market_name.keys()),
                      emitent_id=list(self.emitent_id.keys()),
                      emitent_code=list(self.emitent_code.keys()),
                      emitent_name=list(self.emitent_name.keys())) \
            .read(reference=self.config['reference'], dfrom=self.start_date, dto=self.config['end_date'],
                  price=self.config['price'], volume=self.config['volume'],
                  download_if_not_exists=self.config['download_if_not_exists'], normed=self.config['normed'],
                  daily_returns=self.config['daily_returns'])
