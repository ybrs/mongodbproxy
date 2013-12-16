import sys, time
from pymongo import Connection
import argparse
from multiprocessing import Pool

def mongo_set(data):
    for k, v in data.iteritems():
        collection.insert({'key': k, 'value': v})

def mongo_get(data):
    for k in data.iterkeys():
        val = collection.find_one({'key': k}, fields=('value',)).get('value')

def do_tests(num, tests):
    print "starting tests..."
    # setup dict with key/values to retrieve
    data = {'key' + str(i): 'val' + str(i)*100 for i in range(num)}
    # run tests
    for test in tests:
        start = time.time()
        test(data)
        elapsed = time.time() - start
        print "Completed %s: %d ops in %.2f seconds : %.1f ops/sec" % (test.__name__, num, elapsed, num / elapsed)
    return 1

def main():
    global collection
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo', help='mongo server eg: 127.0.0.1:27017', default="127.0.0.1:27017")
    parser.add_argument('--times', help='times to run tests eg: 1000', default=1000)
    parser.add_argument('--clients', help='how many clients you want to run at the same time eg: 10', default=1)

    args = parser.parse_args()
    host, port = args.mongo.rsplit(':')
    mongo = Connection(*[host, int(port)]).test
    collection = mongo['test']
    collection.ensure_index('key', unique=True)

    tests = [mongo_set, mongo_get]
    client_count = int(args.clients)
    if client_count > 1:
        pool = Pool(client_count)
        def ended(r):
            print r
        for i in range(1,client_count):
            r = pool.apply_async(do_tests, [int(args.times), tests], callback=ended)
        pool.close()
        pool.join()
    else:
        do_tests(int(args.times), tests)

if __name__ == '__main__':
    main()