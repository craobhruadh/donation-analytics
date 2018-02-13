
import pandas as pd
import numpy as np
#from scipy import stats

# # Note:
# The header file can be found here (from the site linked in the Readme)
#
# https://classic.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml

column_names = open('./src/indiv_header_file.csv').read().split(',')
delimiter = '|'
output_filename = 'repeat_donors.txt'

df = pd.read_csv('./input/itcont.txt', names = column_names, dtype={'ZIP_CODE': object, 'TRANSACTION_DT': object}, delimiter=delimiter)
percentile = open('./input/percentile.txt').read().rstrip()
percentile = float(percentile)


# It sounds like, from the challenge, we can just load the data as a file, and not treat it as a stream of
# data? Because if the latter were the case, one can treat the files as such (replacing the file opening with
# whatever pipeline is)

#with open('./donation-analytics/insight_testsuite/tests/test_1/input/itcont.txt') as f:
#    for line in f:
#        tokens = line.split('|')
#        print(tokens)


# From the readme:
# ----------------
# ...while there are many fields in the file that may be interesting, below are the ones that you’ll
# need to complete this challenge:
#
#CMTE_ID: identifies the flier, which for our purposes is the recipient of this contribution
#NAME: name of the donor
#ZIP_CODE: zip code of the contributor (we only want the first five digits/characters)
#TRANSACTION_DT: date of the transaction
#TRANSACTION_AMT: amount of the transaction
#OTHER_ID: a field that denotes whether contribution came from a person or an entity
df = df[['CMTE_ID','NAME', 'ZIP_CODE','TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']]


# While the data dictionary has the ZIP_CODE occupying nine characters, for the
# purposes of the challenge, we only consider the first five characters of the
# field as the zip code
df['ZIP_CODE'] = df['ZIP_CODE'].map(lambda x: str(x)[0:5])


# Because the data set doesn't contain a unique donor id, you should use the
# combination of NAME and ZIP_CODE (again, first five digits) to identify a unique donor
df['UniqueID'] =df.loc[:,'NAME'] + df.loc[:,'ZIP_CODE']



# Because we are only interested in individual contributions, we only want records
# that have the field, OTHER_ID, set to empty. If the OTHER_ID field contains any
# other value, you should completely ignore and skip the entire record
df = df[df['OTHER_ID'].isnull()]


# Other situations you can completely ignore and skip an entire record:
# If TRANSACTION_DT is an invalid date (e.g., empty, malformed)
# If ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
# If the NAME is an invalid name (e.g., empty, malformed)
# If any lines in the input file contains empty cells in the CMTE_ID or TRANSACTION_AMT fields
#
# These two lines should drop any rows with NA values
df.drop('OTHER_ID', axis=1, inplace=True)
df.dropna(inplace=True)


df = df[df['TRANSACTION_DT'].str.len() == 8]
df = df[df['ZIP_CODE'].str.len() == 5]


# Can assume last four characters of transaction date are the year:
df['Year'] = df['TRANSACTION_DT'].map(lambda x: int(str(x)[-4:]))


# Calculate percentile with Nearest Rank: https://en.wikipedia.org/wiki/Percentile#The_nearest-rank_method
#
# note: having looked at the wikipedia definition, what Wikipedia says is "nearest rank" is what
# NumPy thinks is "higher" interpolation, to the best of my understanding
def customPercentile(x):
    return np.percentile(x,percentile, interpolation ='lower')

# Now that the data's actually clean, iterate through the rows
# Note that recalling a specific Donor from a dictionary is an O(1) operation so this should be fine

df_out = pd.DataFrame(columns=['CMTE_ID', 'ZIP_CODE','Year','PercentileValue','TotalContributionValue', 'NumberDonations'])
Donors={}

repeat_count = 0

for index, row in df.iterrows():
    person = row['UniqueID']
    if person in Donors:
        Donors[person] += 1
    else:
        Donors[person] = 1

    if Donors[person] > 1:
        df_out.loc[index] = row
        repeat_count += 1

        # Now find the last three values
        table = pd.pivot_table(df.loc[0:index],index=['CMTE_ID', 'ZIP_CODE', 'Year'], values='TRANSACTION_AMT', aggfunc=[np.sum, len, customPercentile], fill_value=0)
        #df_out.loc[index] = df.apply(GetValuesFromTable, axis=1)

        df_out.loc[index][['TotalContributionValue','NumberDonations','PercentileValue']] = table.loc[row['CMTE_ID'], row['ZIP_CODE'], row['Year']]

df_out.to_csv(path_or_buf='./output/repeat_donors.txt', header=False, index=False, sep=delimiter)
