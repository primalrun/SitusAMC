from itertools import islice

in_file = r'C:\Users\jasonwalker\OneDrive - SitusAMC\Documents\project\baseline\source_file\202110\GNMA_HMB_LL_MON_rl2_202110_001.txt'
disclosure_sequence_number = '1000001312'
sequence_number_suffix = '064'

with open(in_file) as f:
    data = [line for line in f if line[0] == 'L'
            and line[11:21] == disclosure_sequence_number
            and line[21:24] == sequence_number_suffix][0]

dsn = data[11:21]
sns = data[21:24]
original_hecm_loan_balance = data[53:65]
hecm_original_funding_date = data[143:149]
as_of_date = data[372:378]
mca = data[332:345]
issuer_id = data[7:11]
rate_reset_frequency = data[82:83]
pool_id = data[1:7]
payment_reason = data[51:53]
mip_basis_points = data[329:332]
participation_upb = data[108:120]
participation_interest_rate = data[120:125]
monthly_servicing_fee_amount = data[282:294]
hecm_loan_current_interest_rate = data[103:108]
current_hecm_loan_balance = data[65:77]
remaining_available_line_of_credit = data[270:282]

print(dsn)
print(sns)
print(original_hecm_loan_balance)
print(hecm_original_funding_date)
print(as_of_date)
print(mca)
print(issuer_id)
print(rate_reset_frequency)
print(pool_id)
print(payment_reason)
print(mip_basis_points)
print(participation_upb)
print(participation_interest_rate)
print(monthly_servicing_fee_amount)
print(hecm_loan_current_interest_rate)
print(current_hecm_loan_balance)
print(remaining_available_line_of_credit)










