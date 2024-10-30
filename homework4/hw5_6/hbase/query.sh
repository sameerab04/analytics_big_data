#!/bin/bash

# Return the bodies of all emails for a user of k.allen.
echo "scan 'omo6093_email_table', {COLUMNS => ['details:to', 'content:body'], FILTER => \"SingleColumnValueFilter('details', 'to', =, 'binary:k..allen@enron.com')\"}" | hbase shell > allen_emails.txt

# Return the bodies of all emails written during Dec 2001
echo "scan 'omo6093_email_table', {COLUMNS => ['details:date', 'content:body'], FILTER => \"SingleColumnValueFilter('details', 'date', =, 'substring:Dec 2001')\"}" | hbase shell > Dec2001_emails.txt

# Return the bodies of all emails of k.allen during Dec 2001
echo "scan 'omo6093_email_table', {COLUMNS => ['details:to', 'details:date', 'content:body'], FILTER => \"(SingleColumnValueFilter('details', 'to', =, 'binary:k..allen@enron.com') AND SingleColumnValueFilter('details', 'date', =, 'substring:Dec 2001'))\"}" | hbase shell > allen_Dec2001_emails.txt

# Disable and drop the table
echo "disable 'omo6093_email_table'" | hbase shell
echo "drop 'omo6093_email_table'" | hbase shell

exit