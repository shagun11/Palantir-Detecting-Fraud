def parseStartLines(employee, jobs_map, counter):
  """ Parses all the jobs which have started 
      and creates a new entry in the hash map for them
  """
  
  if employee not in jobs_map or jobs_map[employee][2] > 0:
    jobs_map[employee] = [counter, -1, -1] # start_line, job_id, end_line
  else:
    # Formats the current job if it is a batch
    if not isinstance(jobs_map[employee][0], list):
      temp = jobs_map[employee][0]
      jobs_map[employee][0] = [temp]
    jobs_map[employee][0].append(counter)


def checkSuspicious(key, start_lines, ids, employee, counter):
  """ Checks if the current batch is suspicious or not
      Also filters out the batches which are shortened inside the
      suspicious batch
  """
  
  _, job_id, end_line = key
  jobs = []   
  # Checks if all invoice ids are lesser than the current id
  all_invoices = all(id < job_id for id in ids)
  
  # Filters the job ids which are greater than the job id
  fraud_ids = list(filter(lambda x: x > job_id, ids))
  fraud_ids_len = len(fraud_ids)
  
  # Filters the lines which started later than previous job ended
  fraud_start = list(filter(lambda x: x > end_line, start_lines))
  fraud_lines = len(fraud_start)
  
  if len(start_lines) == fraud_lines and all_invoices:
    # Append all the shortened jobs in a batch
    for line in start_lines:
      jobs.append('{};{};{}'.format(str(line), employee, 'SHORTENED_JOB'))
      
  elif fraud_ids_len < fraud_lines:
    if all_invoices:
      for f in fraud_start:
        jobs.append('{};{};{}'.format(str(f), employee, 'SHORTENED_JOB'))
    jobs.append('{};{};{}'.format(str(counter), employee, 'SUSPICIOUS_BATCH'))
    
  return jobs


def parseEndedJobs(jobs_map, value, employee, counter, ended_jobs):
  """ It parses the jobs which are ended and
      updates the map accordingly
  """
  
  fraud_jobs = []
  value = list(map(int, value))
  ids = value if len(value) > 1 else value[0]
  jobs_map[employee][2] = counter
  jobs_map[employee][1] = ids
  start, invoice_ids, end = jobs_map[employee]
  
  # Checks the current job with the other job ids
  for key in reversed(ended_jobs):
    job_id, end_line = max(key[1]) if isinstance(key[1], list) else key[1], key[2]
    
    # Checks if the job is batch
    if len(value) > 1:
      suspicious_batch = checkSuspicious(key, start, invoice_ids, employee, counter)
      fraud_jobs.extend(suspicious_batch)
      if len(suspicious_batch) > 0:
        break
      
    elif len(value) == 1 and invoice_ids < job_id and start > end_line:
      fraud_jobs.append('{};{};{}'.format(str(start), employee, 'SHORTENED_JOB'))
      break
    
  if len(fraud_jobs) < 1:
    ended_jobs.append([employee, invoice_ids, end])
  return fraud_jobs

    
def findViolations(datafeed):
  """ It finds the jobs which are fraudalent
  """
  
  jobs_map, fraud_jobs, ended_jobs = dict(), [], []
  
  for i, data in enumerate(datafeed):
    emp_name, value = data.split(';')
    value = value.split(',')
    
    if value[0] == 'START': 
      # Create an entry for a new job 
      parseStartLines(emp_name, jobs_map, i+1)
    else: 
      # Updates the new employee with the end line and job ids
      fraud_jobs.extend(parseEndedJobs(jobs_map, value, emp_name, i+1, ended_jobs))

  return fraud_jobs



