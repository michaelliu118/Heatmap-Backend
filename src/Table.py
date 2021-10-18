import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import io
import base64
import json

plt.switch_backend('agg')

class Table:

    def __init__(self, azure_db_engine=None):
        if not azure_db_engine:
            raise Exception('Initiate this class with an Azure database engine!')
        else:
            self.engine = azure_db_engine
            self.original_table = None
            self.calling_method_sequence_identifier = 0
            self.K = None
            self.ATA_table = None
            self.value_table = None

    # Get the table by passing a sql query and return a pandas dataframe with fillna(0)
    def query_table(self, query):

        try:
            self.original_table = pd.read_sql(query, self.engine, index_col='ATA')
            # uniform the null values to np.NAN
            self.original_table.fillna(np.NAN, inplace=True)
            self.calling_method_sequence_identifier = 1
            return self.original_table
        except ValueError:
            raise Exception('Invalid query or engine!')

    # for each operator, select only the K number of ATA metric
    def select_top_K_number_for_each_operator(self, K):

        if self.calling_method_sequence_identifier == 1:
            self.calling_method_sequence_identifier = 2

            # construct a dataframe storing ATA number
            self.ATA_table = pd.DataFrame(index=[i for i in range(len(self.original_table.index))])

            # construct a dataframe storing metric values
            self.value_table = pd.DataFrame(index=[i for i in range(len(self.original_table.index))])

            # At this stage the index of original table should be ATA
            for col in self.original_table.columns:
                dummy = self.original_table[col].sort_values(ascending=False).reset_index()

                # reflect null in values to ATA index in dummy
                dummy['ATA'] = dummy.apply(lambda x: 'null' if np.isnan(x[col]) else x['ATA'], axis=1)

                # add the ATA number to ATA table
                self.ATA_table[col] = dummy['ATA']

                # add the corresponding value to value table
                self.value_table[col] = dummy[col]

            # Keep only the requested K number of rows
            self.ATA_table.drop(self.ATA_table.index[K + 1:], inplace=True)
            self.value_table.drop(self.value_table.index[K + 1:], inplace=True)
            self.value_table.fillna(0, inplace=True)

        else:
            raise Exception('Please pass a SQL query to "query_table')

    def generate_heatmap_html(self):

        if self.calling_method_sequence_identifier == 2:

            # Following try/except is used to add header to the heatmap
            try:
                with open('./static/region.json') as infile:
                    data = json.load(infile)
                grouping_dict = dict()
                for col in self.value_table.columns:
                    if col in data:
                        region = data[col]
                        grouping_dict[region] = grouping_dict.get(region, []) + [col]
                    else:
                        grouping_dict['unknown'] = grouping_dict.get('unknown', []) + [col]
                        data[col] = "unknown"

                # Construct the array of new sequence of columns
                reorganize_columns = []
                for k, v in grouping_dict.items():
                    reorganize_columns += v

                # re-organize column sequence for both tables
                self.value_table = self.value_table[reorganize_columns]
                self.ATA_table = self.ATA_table[reorganize_columns]

                # Add the region as the second column layer
                self.value_table.columns = pd.MultiIndex. \
                    from_tuples([(data[col], col) for col in self.value_table.columns])

            except (FileNotFoundError, RuntimeError, KeyError):
                pass

            # Generating the heatmap
            styler = self.value_table.style.background_gradient(axis=None). \
                set_properties(**{'font-size': '20px', 'width': '300px', 'table-layout': 'fixed'}). \
                set_table_styles([{'selector': ' thead > tr > th', 'props': [('border-style', 'solid'),
                                                                             ('border-width', '1px'),
                                                                             ('border-color', '#2b6f8a'),
                                                                             ('text-align', 'center')]}])
            html_string = styler.to_html()
            row_length = len(self.value_table.columns)
            count = 0
            start = 0
            while True:
                start = html_string.find('</td>', start)
                if start < 0:
                    break
                count += 1
                current_row = count // row_length
                current_column = count % row_length - 1
                if current_column == -1:
                    current_row -= 1
                forward = start - 1
                while forward >= 0:
                    if html_string[forward] == '>':
                        break
                    forward -= 1
                html_string = html_string[:forward + 1] + \
                              str(self.ATA_table.iloc[current_row, current_column]) + \
                              html_string[start:]
                start += 10
            # print(html_string)

            return html_string
        else:
            raise Exception('Please call method "select_top_K_number_for_each_operator" first!')

    # The output of this method is a base64 encoded png image
    def generate_heatmap_colorBar_image_encode(self):

        if self.calling_method_sequence_identifier == 2:
            # minimum/max value over entire table
            minMetric = self.value_table.min().min()
            maxMetric = self.value_table.max().max()
            interval = (maxMetric - minMetric) / 10

            # construct the colorBar
            colorBar = pd.DataFrame([[i for i in range(11)]])
            colorBar.columns = [str(round(minMetric + interval * i, 2)) for i in range(0, 11)]

            # plot the heatmap on dataframe
            sns.heatmap(colorBar, cmap='PuBu', cbar=False, square=True)

            # encode the heatmap into base 64 string
            img = io.BytesIO()
            plt.savefig(img, bbox_inches='tight')
            heatmap_src = "data:image/png;base64,{}".format(base64.b64encode(img.getvalue()).decode())
            return heatmap_src
        else:
            raise Exception('Please call method "select_top_K_number_for_each_operator" first!')
