import datetime
import subprocess
import json
import boto3

if __name__ == "__main__":

    sess = boto3.Session(profile_name='default')
    client = sess.client('s3')
    scripts = {'standard': 'scripts_to_time/extract_tws.py',
               'parallelized': 'scripts_to_time/extract_tws_parallelized.py'}
    input_sizes = [100, 1000]
    n_trials = 1

    # get results
    for script_type, script_fpath in scripts.items():
        print('==================================')
        print(f'Benchmarking {script_type}.\n\nExecuting {script_fpath}\n...\n')
        results = {input_size: {'times': []} for input_size in input_sizes}
        for input_size in input_sizes:
            print(f'Runninng {n_trials} trials for n={input_size}...\n')
            for t in range(0, n_trials):
                start = datetime.datetime.now()
                subprocess.run(['python',
                                script_fpath,
                                str(input_size)])
                end = datetime.datetime.now()

                # add time
                diff = end - start
                results[input_size]['times'].append(diff.total_seconds())

        output_fpath = f'./data/times_{script_type}.json'
        print(f'Saving results to {output_fpath}')

        with open(output_fpath, 'w') as f:
            f.write(json.dumps(results))

        with open(output_fpath, 'r') as f:
            client.put_object(Body=f.read(),
                              Bucket='awspot-instances',
                              Key=f'tw_extraction_times/times_{script_type}.json')
