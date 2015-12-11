import test_external, mock, external, unittest

#print test_external._construct_mock_bigquery_response([{'foo': 'bar1', 'faz': 'baz1'}, {'foo': 'bar2', 'faz': 'baz2'}])


#returns 
#{'rows': [{'f': [{'v': 'baz1'}, {'v': 'bar1'}]}, {'f': [{'v': 'baz2'}, {'v': 'bar2'}]}], 'totalRows': 2, 'schema': {'fields': [{'name': 'faz'}, {'name': 'foo'}]}}


#mock= Mock()
#mock.side_effect= external.BigQueryCommunicationError()


# def printException(): 
# 	a= []
# 	try: 
# 		a[0]

# 	except Exception as e: 
# 		print e
# 		raise external.BigQueryCommunicationError('Query failed with %s:\n %s', e.message)


# printException()


# import argparse

# from apiclient.discovery import build
# from apiclient.errors import HttpError
# from oauth2client.client import GoogleCredentials


# def main(project_id):
#     # [START build_service]
#     # Grab the application's default credentials from the environment.
#     credentials = GoogleCredentials.get_application_default()
#     # Construct the service object for interacting with the BigQuery API.
#     bigquery_service = build('bigquery', 'v2', credentials=credentials)
#     # [END build_service]

#     try:
#         # [START run_query]
#         query_request = bigquery_service.jobs()
#         query_data = {
#             'query': (
#                 'SELECT TOP(corpus, 10) as title, '
#                 'COUNT(*) as unique_words '
#                 'FROM [publicdata:samples.shakespeare];')
#         }

#         query_response = query_request.query(
#             projectId=project_id,
#             body=query_data).execute()

#         print "QUERY_RESPONSE"
#         print query_response
#         # [END run_query]

#         # [START print_results]
#         print('Query Results:')
#         for row in query_response['rows']:
#             print('\t'.join(field['v'] for field in row['f']))
#         # [END print_results]

#     except HttpError as err:
#         print('Error: {}'.format(err.content))
#         raise err


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(
#         description=__doc__,
#         formatter_class=argparse.RawDescriptionHelpFormatter)
#     parser.add_argument('project_id', help='Your Google Cloud Project ID.')

#     args = parser.parse_args()

#     main(args.project_id)
# # [END all]
