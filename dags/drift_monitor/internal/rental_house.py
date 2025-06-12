import pandas as pd


class data:

    def trace_preparation(self, json_data_list):
        request_list = []
        response_list = []

        for data in json_data_list:
            id = data['id']
            if 'inputs' in data['data']:
                request_list.append([id, data['data']['inputs'][0]['data'][0]])
            elif 'outputs' in data['data']:
                response_list.append(
                    [id, data['data']['outputs'][0]['data'][0]])
            else:
                pass

        request_df = pd.DataFrame(request_list)
        response_df = pd.DataFrame(response_list)
        print(f"AAAAAAAA{request_df.head()}")

        id_list = request_df.iloc[:, 0].tolist()
        merged_data_list = []
        for id in id_list:
            if not response_df[response_df[0] == id].empty:
                req = request_df[request_df[0] == id][1].values[0]
                res = response_df[response_df[0] == id][1].values[0]
                merged_data_list.append({
                    'id': id,
                    'request': req,
                    'response': res
                })
        trace_merged_df = pd.DataFrame(merged_data_list)

        # Define new column names
        request_columns = [
            'square_footage', 'bedrooms', 'bathrooms', 'furnished'
        ]

        # Expand 'request' list into separate columns
        expanded = pd.DataFrame(trace_merged_df['request'].tolist(),
                                columns=request_columns)

        # Combine with original DataFrame
        trace_df = pd.concat(
            [expanded,
             trace_merged_df.drop(columns=['request', 'id'])],
            axis=1)
        trace_df = trace_df.rename(columns={"response": "monthly_rent"})
        trace_pred_df = trace_df[["monthly_rent"]].copy()
        trace_query_df = trace_df.drop(columns=["monthly_rent"], axis=1)

        return trace_query_df, trace_pred_df
