import json
import logging
import os
import requests
from retrying import retry
import time
import unittest
import uuid

class CookTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    @retry(stop_max_delay=120000, wait_fixed=1000)
    def wait_for_job(self, job_id, status):
        job = self.session.get('%s/rawscheduler?job=%s' % (self.cook_url, job_id))
        self.assertEqual(200, job.status_code)
        job = job.json()[0]
        if not job['status'] == status:
            error_msg = 'Job %s had status %s - expected %s' % (job_id,
                                                                job['status'],
                                                                status)
            self.logger.debug(error_msg)
            raise RuntimeError(error_msg)
        else:
            self.logger.debug('Job %s has status %s - %s', job_id, status,
                              job)
        return job

    def minimal_job(self, **kwargs):
        job = {
            'max_retries': 1,
            'mem': 10,
            'cpus': 1,
            'uuid': str(uuid.uuid4()),
            'command': 'echo hello',
            'name': 'echo',
            'priority': 1
        }
        job.update(kwargs)
        return job

    def minimal_group(self, **kwargs):
        group = {"uuid" : str(uuid.uuid4())}
        group.update(kwargs)
        return group

    def setUp(self):
        self.cook_url = os.getenv('COOK_SCHEDULER_URL', 'http://localhost:12321')
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def test_basic_submit(self):
        job_spec = self.minimal_job()
        request_body = {'jobs': [ job_spec ]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEqual('success', job['instances'][0]['status'])

    def test_failing_submit(self):
        job_spec = self.minimal_job(command='exit 1')
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        self.assertEqual(201, resp.status_code)
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEqual(1, len(job['instances']))
        self.assertEqual('failed', job['instances'][0]['status'])

    def test_max_runtime_exceeded(self):
        job_spec = self.minimal_job(command='sleep 60', max_runtime=5000)
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        self.assertEqual(201, resp.status_code)
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEqual(1, len(job['instances']))
        self.assertEqual('failed', job['instances'][0]['status'])
        self.assertEqual(2003, job['instances'][0]['reason_code'])

    # load a job by UUID using GET /rawscheduler
    def get_job(self, uuid):
        return self.session.get('%s/rawscheduler?job=%s' % (self.cook_url, uuid)).json()[0]

    def test_get_job(self):
        # schedule a job
        job_spec = self.minimal_job() 
        request_body = {'jobs': [job_spec]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        self.assertEqual(201, resp.status_code)

        # query for the same job & ensure the response has what it's supposed to have
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEquals(job_spec['mem'], job['mem'])
        self.assertEquals(job_spec['max_retries'], job['max_retries'])
        self.assertEquals(job_spec['name'], job['name'])
        self.assertEquals(job_spec['priority'], job['priority'])
        self.assertEquals(job_spec['uuid'], job['uuid'])
        self.assertEquals(job_spec['cpus'], job['cpus'])
        self.assertTrue('labels' in job)
        self.assertEquals(9223372036854775807, job['max_runtime'])
        # 9223372036854775807 is MAX_LONG(ish), the default value for max_runtime
        self.assertEquals('success', job['state'])
        self.assertTrue('env' in job)
        self.assertTrue('framework_id' in job)
        self.assertTrue('ports' in job)
        self.assertTrue('instances' in job)
        self.assertEquals('completed', job['status'])
        self.assertTrue(isinstance(job['submit_time'], int))
        self.assertTrue('uris' in job)
        self.assertTrue('retries_remaining' in job)
        instance = job['instances'][0]
        self.assertTrue(isinstance(instance['start_time'], int))
        self.assertTrue('executor_id' in instance)
        self.assertTrue('hostname' in instance)
        self.assertTrue('slave_id' in instance)
        self.assertTrue(isinstance(instance['preempted'], bool))
        self.assertTrue(isinstance(instance['end_time'], int))
        self.assertTrue(isinstance(instance['backfilled'], bool))
        self.assertTrue('ports' in instance)
        self.assertEquals('completed', job['status'])
        self.assertTrue('task_id' in instance)

    def determine_user(self):
        job_spec = self.minimal_job()
        request_body = {'jobs': [ job_spec ]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)
        return self.get_job(job_spec['uuid'])['user']

    def test_list_jobs_by_state(self):
        # schedule a bunch of jobs in hopes of getting jobs into different statuses
        request_body = {'jobs': [self.minimal_job(command="sleep %s" % i) for i in range(1,20)]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)

        # let some jobs get scheduled
        time.sleep(10)
        user = self.determine_user()

        for state in ['waiting', 'running', 'completed']:
            resp = self.session.get('%s/list?user=%s&state=%s' % (self.cook_url, user, state))
            self.assertEqual(200, resp.status_code)
            jobs = resp.json()
            for job in jobs:
                #print "%s %s" % (job['uuid'], job['status'])
                self.assertEquals(state, job['status'])

    def test_list_jobs_by_time(self):
        # schedule two jobs with different submit times
        job_specs = [self.minimal_job() for _ in range(2)];

        request_body = {'jobs': [job_specs[0]]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)

        time.sleep(1)

        request_body = {'jobs': [job_specs[1]]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=request_body)
        self.assertEqual(resp.status_code, 201)

        submit_times = [self.get_job(job_spec['uuid'])['submit_time'] for job_spec in job_specs]

        user = self.determine_user()

        # start-ms and end-ms are exclusive

        # query where start-ms and end-ms are the submit times of jobs 1 & 2 respectively
        resp = self.session.get('%s/list?user=%s&state=waiting&start-ms=%s&end-ms=%s'
                % (self.cook_url, user, submit_times[0]-1, submit_times[1]+1))
        self.assertEqual(200, resp.status_code)
        jobs = resp.json()
        self.assertTrue(any(job for job in jobs if job['uuid'] == job_specs[0]['uuid']))
        self.assertTrue(any(job for job in jobs if job['uuid'] == job_specs[1]['uuid']))

        # query just for job 1
        resp = self.session.get('%s/list?user=%s&state=waiting&start-ms=%s&end-ms=%s'
                % (self.cook_url, user, submit_times[0]-1, submit_times[1]))
        self.assertEqual(200, resp.status_code)
        jobs = resp.json()
        self.assertTrue(any(job for job in jobs if job['uuid'] == job_specs[0]['uuid']))
        self.assertFalse(any(job for job in jobs if job['uuid'] == job_specs[1]['uuid']))

        # query just for job 2
        resp = self.session.get('%s/list?user=%s&state=waiting&start-ms=%s&end-ms=%s'
                % (self.cook_url, user, submit_times[0], submit_times[1]+1))
        self.assertEqual(200, resp.status_code)
        jobs = resp.json()
        self.assertFalse(any(job for job in jobs if job['uuid'] == job_specs[0]['uuid']))
        self.assertTrue(any(job for job in jobs if job['uuid'] == job_specs[1]['uuid']))

        # query for neither
        resp = self.session.get('%s/list?user=%s&state=waiting&start-ms=%s&end-ms=%s'
                % (self.cook_url, user, submit_times[0], submit_times[1]))
        self.assertEqual(200, resp.status_code)
        jobs = resp.json()
        self.assertFalse(any(job for job in jobs if job['uuid'] == job_specs[0]['uuid']))
        self.assertFalse(any(job for job in jobs if job['uuid'] == job_specs[1]['uuid']))

    def test_cancel_job(self):
        job_spec = self.minimal_job(command='sleep 300')
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        self.wait_for_job(job_spec['uuid'], 'running')
        resp = self.session.delete(
            '%s/rawscheduler?job=%s' % (self.cook_url, job_spec['uuid']))
        self.assertEqual(204, resp.status_code)
        job = self.session.get(
            '%s/rawscheduler?job=%s' % (self.cook_url, job_spec['uuid'])).json()[0]
        self.assertEqual('failed', job['state'])

    def test_change_retries(self):
        job_spec = self.minimal_job(command='sleep 10')
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        self.wait_for_job(job_spec['uuid'], 'running')
        resp = self.session.delete(
            '%s/rawscheduler?job=%s' % (self.cook_url, job_spec['uuid']))
        self.assertEqual(204, resp.status_code)
        job = self.session.get(
            '%s/rawscheduler?job=%s' % (self.cook_url, job_spec['uuid'])).json()[0]
        self.assertEqual('failed', job['state'])
        resp = self.session.put('%s/retry' % self.cook_url, json={'retries': 2, 'jobs': [job_spec['uuid']]})
        self.assertEqual(201, resp.status_code, resp.text)
        job = self.session.get(
            '%s/rawscheduler?job=%s' % (self.cook_url, job_spec['uuid'])).json()[0]
        self.assertEqual('waiting', job['status'])
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEqual('success', job['state'])

    def test_cancel_instance(self):
        job_spec = self.minimal_job(command='sleep 10', max_retries=2)
        resp = self.session.post('%s/rawscheduler' % self.cook_url,
                                 json={'jobs': [job_spec]})
        job = self.wait_for_job(job_spec['uuid'], 'running')
        task_id = job['instances'][0]['task_id']
        resp = self.session.delete(
            '%s/rawscheduler?instance=%s' % (self.cook_url, task_id))
        self.assertEqual(204, resp.status_code)
        job = self.wait_for_job(job_spec['uuid'], 'completed')
        self.assertEqual('success', job['state'])

    def test_implicit_group(self):
        group_uuid = str(uuid.uuid4())
        job_a = self.minimal_job(group=group_uuid)
        job_b = self.minimal_job(group=group_uuid)
        data = {'jobs':[job_a, job_b]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=data)
        self.assertEqual(resp.status_code, 201)
        jobs = self.session.get('%s/rawscheduler?job=%s&job=%s' % 
                                (self.cook_url, job_a['uuid'], job_b['uuid']))
        self.assertEqual(200, jobs.status_code)
        jobs = jobs.json()
        self.assertEqual(group_uuid, jobs[0]['groups'][0])
        self.assertEqual(group_uuid, jobs[1]['groups'][0])
        self.wait_for_job(job_a['uuid'], 'completed')
        self.wait_for_job(job_b['uuid'], 'completed')


    def test_explicit_group(self):
        group_spec = self.minimal_group()
        job_a = self.minimal_job(group=group_spec["uuid"])
        job_b = self.minimal_job(group=group_spec["uuid"])
        data = {'jobs':[job_a, job_b], 'groups':[group_spec]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=data)
        self.assertEqual(resp.status_code, 201)
        jobs = self.session.get('%s/rawscheduler?job=%s&job=%s' % 
                                (self.cook_url, job_a['uuid'], job_b['uuid']))
        self.assertEqual(200, jobs.status_code)
        jobs = jobs.json()
        self.assertEqual(group_spec['uuid'], jobs[0]['groups'][0])
        self.assertEqual(group_spec['uuid'], jobs[1]['groups'][0])
        self.wait_for_job(job_a['uuid'], 'completed')
        self.wait_for_job(job_b['uuid'], 'completed')

    def test_straggler_handling(self):
        straggler_handling = {
                'type' : 'quantile-deviation',
                'parameters' : {
                    'quantile' : 0.5,
                    'multiplier' : 2.0
                    }
                }
        group_spec = self.minimal_group(straggler_handling=straggler_handling)
        job_fast = self.minimal_job(group=group_spec["uuid"])
        job_slow = self.minimal_job(group=group_spec["uuid"], command='sleep 120')
        data = {'jobs':[job_fast, job_slow], 'groups':[group_spec]}
        resp = self.session.post('%s/rawscheduler' % self.cook_url, json=data)
        self.assertEqual(resp.status_code, 201)
        self.wait_for_job(job_fast['uuid'], 'completed')
        self.wait_for_job(job_slow['uuid'], 'completed')
        jobs = self.session.get('%s/rawscheduler?job=%s&job=%s' % 
                                (self.cook_url, job_fast['uuid'], job_slow['uuid']))
        self.assertEqual(200, jobs.status_code)
        jobs = jobs.json()
        self.logger.debug('Loaded jobs %s', jobs)
        self.assertEqual('success', jobs[0]['state'])
        self.assertEqual('failed', jobs[1]['state'])
        self.assertEqual(2004, jobs[1]['instances'][0]['reason_code'])

