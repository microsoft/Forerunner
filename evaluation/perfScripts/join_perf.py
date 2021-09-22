import os
from os import path
import argparse
from bisect import bisect_left

parser = argparse.ArgumentParser("")
parser.add_argument("-b", dest="base_filename")
parser.add_argument("-f", dest="reuse_filename")
parser.add_argument("-o", dest="output_path")

args = parser.parse_args()

base_filename = args.base_filename
reuse_filename = args.reuse_filename
outdir = args.output_path

result_filename = path.join(outdir, "TxSpeedupResult.txt")
output_filename = path.join(outdir, "AllPerfTxLog.joined.txt")

IGNORE_LINE_COUNT = 10000  # 100000 # 2000000
SIZE_LIMIT = 2000000  # 000000

baselines = open(base_filename).readlines()[IGNORE_LINE_COUNT:IGNORE_LINE_COUNT + SIZE_LIMIT]
reuselines = open(reuse_filename).readlines()[IGNORE_LINE_COUNT:IGNORE_LINE_COUNT + SIZE_LIMIT]

print "tx count of baseline:", len(baselines), "tx count of forerunner:", len(reuselines)


# print baselines[0]
# print reuselines[0]


def parseKVLine(line):
    parts = line.strip().split()
    assert len(parts) % 2 == 0
    kvs = dict([(k, eval(v)) for k, v in zip(parts[::2], parts[1::2])])
    return kvs


#
# print parseKVLine(baselines[0])
# print parseKVLine(reuselines[0])


class BaseRecord:
    def __init__(self, line):
        self.kvs = parseKVLine(line)
        self.line = line.strip()


class ReuseRecord:
    def __init__(self, line):
        kvs = self.kvs = parseKVLine(line)
        assert "delay" in self.kvs
        self.line = line.strip()
        self.fullStatus = "-".join(
            [kvs["baseStatus"]] + [kvs[k] for k in "hitType mixHitType traceHitType missType".split() if k in kvs])

    def addBase(self, br):
        assert br.kvs["id"] == self.kvs["id"]
        assert br.kvs["tx"] == self.kvs["tx"]
        self.base = br.kvs["base"]
        assert self.kvs["gas"] == br.kvs["gas"]
        self.speedup = float(self.base) / self.kvs["reuse"]
        return self

    def getNewLine(self):
        # p0, p2 = self.parts[:7], self.parts[7:]
        p1 = ["base", self.base, "speedup", self.speedup]
        # parts = p0 + p1 + p2
        return self.line + " " + " ".join(map(str, p1)) + "\n"

    def getSpeedupLine(self):
        p1 = ["id", self.kvs["id"], "tx", self.kvs["tx"], "speedup", self.speedup, "reuse", self.kvs["reuse"], "base",
              self.base, "gas", self.kvs["gas"]]
        return " ".join(map(str, p1)) + "\n"


# br = BaseRecord(baselines[0])
# print br.kvs["base"], br.kvs["id"], br.kvs["tx"], br.kvs["gas"]
#
# rr = ReuseRecord(reuselines[0])
# print rr.kvs["reuse"], rr.kvs["id"], rr.kvs["tx"], rr.kvs["baseStatus"]

base_records = map(BaseRecord, baselines)
reuse_records = map(ReuseRecord, reuselines)

base_ids = set([r.kvs["id"] for r in base_records])
reuse_ids = set([r.kvs["id"] for r in reuse_records])
common_ids = base_ids.intersection(reuse_ids)

id2base = dict([(r.kvs["id"], r) for r in base_records])

pairs = []
for r in reuse_records:
    if r.kvs["id"] in common_ids:
        br = id2base[r.kvs["id"]]
        pairs.append((r, br))

print "common", len(pairs)

merged_records = [r.addBase(b) for r, b in pairs]
merged_lines = []
speedup_lines = []
all_speedups = []
trace_slowdowns = []
total_trace_time = 0
total_trace_all_time = 0
total_trace_count = 0
total_base_time = 0
max_speedup = 0
max_line = ""
for r in merged_records:
    line = r.getNewLine()
    merged_lines.append(line)
    kvs = r.kvs

    all_speedups.append(r.speedup)
    if r.speedup > max_speedup:
        max_speedup = r.speedup
        max_line = line
    speedup_lines.append(r.getSpeedupLine())
    if "sD" in kvs:
        sd = kvs["sD"]
        md = kvs["mD"]
        tc = kvs["tC"]
        td = sd + md
        slowdown = td / float(r.base)
        trace_slowdowns.append(slowdown)
        total_trace_time += td
        total_trace_all_time += (td * tc)
        total_trace_count += tc
        total_base_time += r.base

all_speedups = sorted(all_speedups)
m_speedup = all_speedups[-1]
assert m_speedup == max_speedup
lenind = len(all_speedups) - 1
p999 = all_speedups[int(lenind * 0.999)]
p995 = all_speedups[int(lenind * 0.995)]
p99 = all_speedups[int(lenind * 0.99)]
p95 = all_speedups[int(lenind * 0.95)]
g100 = bisect_left(all_speedups, 100)
g100 = 1 - float(g100) / lenind


# merged_lines = [r.getNewLine() for r in merged_records]

def avg(mrecords):
    baseTotal = 0
    raTotal = 0
    for r in mrecords:
        baseTotal += r.base
        raTotal += r.kvs["reuse"]
    if raTotal == 0:
        return 0
    return float(baseTotal) / float(raTotal)


def GetSavedInsPercent(mrecords):
    totalIns = 0
    execIns = 0
    totalOps = 0
    for r in mrecords:
        # if "Hit" == r.kvs["baseStatus"] and "Trace" == r.kvs["hitType"] :
        totalIns += r.kvs.get("tN", 0)
        execIns += r.kvs.get("eN", 0)
        totalOps += r.kvs.get("pN", 0)
    saved = totalIns - execIns
    if totalIns == 0:
        assert totalOps == 0
        return 0, 0, 0, 0, 0
    return saved / float(totalIns), (totalOps - execIns) / float(totalOps), execIns, totalIns, totalOps


def GetSavedLoadsPercent(mrecords):
    totalDetail = 0
    actualDetail = 0
    actualAccount = 0
    for r in mrecords:
        totalDetail += r.kvs.get("fPR", 0)
        totalDetail += r.kvs.get("fPRm", 0)
        actualDetail += r.kvs.get("fAR", 0)
        actualDetail += r.kvs.get("fARm", 0)
        actualAccount += r.kvs.get("aR", 0)
        actualAccount += r.kvs.get("aRm", 0)
    if totalDetail == 0:
        return 0, 0, 0, 0
    return (totalDetail - actualDetail - actualAccount) / float(
        totalDetail), "actualDetail", actualDetail, "actualAccount", actualAccount, "totalDetail", totalDetail


def GetSavedStoresPercent(mrecords):
    totalDetail = 0
    actualDetail = 0
    actualAccount = 0
    for r in mrecords:
        totalDetail += r.kvs.get("fPW", 0)
        totalDetail += r.kvs.get("fPWm", 0)
        actualDetail += r.kvs.get("fAW", 0)
        actualDetail += r.kvs.get("fAWm", 0)
        actualAccount += r.kvs.get("aW", 0)
        actualAccount += r.kvs.get("aWm", 0)
    if totalDetail == 0:
        return 0, 0, 0, 0
    # return (totalDetail - actualDetail - actualAccount) / float(totalDetail), actualDetail, actualAccount, totalDetail
    return (totalDetail - actualDetail - actualAccount) / float(
        totalDetail), "actualDetail", actualDetail, "actualAccount", actualAccount, "totalDetail", totalDetail


result_out = open(result_filename, "w")


def output(*args):
    line = " ".join(map(str, args)) + "\n"
    print line,
    result_out.write(line)


output(result_filename)


# print merged_lines[0]


def isPartiallyCorect(kvs):
    status = kvs["baseStatus"]
    if not status.startswith("Hit"):
        return False
    status = kvs["hitType"]
    if "Trace" == status:
        tstatus = kvs["traceHitType"]
        if tstatus == "OpHit":
            return True
    if "Mix" == status:
        mstatus = kvs["mixHitType"]
        if "Delta" in mstatus:
            return True
    return False


def isFullyCorrect(kvs):
    status = kvs["baseStatus"]
    if not status.startswith("Hit"):
        return False
    if isPartiallyCorect(kvs):
        return False
    return True


def groupByFullStatus(records):
    ret = {}
    for r in records:
        ret.setdefault(r.fullStatus, []).append(r)
    return ret


fully_correct = [r for r in merged_records if isFullyCorrect(r.kvs)]
partially_correct = [r for r in merged_records if isPartiallyCorect(r.kvs)]

wrong = [r for r in merged_records if not r.kvs["baseStatus"].startswith("Hit")]
miss = [r for r in merged_records if r.kvs["baseStatus"] == "Miss"]
no_prediction = [r for r in merged_records if r.kvs["baseStatus"] == "NoListen" or r.kvs["baseStatus"] == "NoPreplay"]
no_listen = [r for r in merged_records if r.kvs["baseStatus"] == "NoListen"]
no_preplay = [r for r in merged_records if r.kvs["baseStatus"] == "NoPreplay"]

all_base_time = float(sum([r.base for r in merged_records]))
fully_correct_base_time = sum([r.base for r in fully_correct])
partially_correct_base_time = sum([r.base for r in partially_correct])
wrong_base_time = sum([r.base for r in wrong])
miss_base_time = sum([r.base for r in miss])
no_prediction_base_time = sum([r.base for r in no_prediction])
no_listen_base_time = sum([r.base for r in no_listen])
no_preplay_base_time = sum([r.base for r in no_preplay])

output("-----")
output("## Main Results")
output("")
output("count of all txs: ", len(merged_records))
output("")
output("overall speedup", avg([r for r in merged_records]))
output("effective speedup", avg([r for r in merged_records if not r.kvs["baseStatus"] == "NoListen"]))


output("")
output("satisfied ratio of all txs:", (len(fully_correct) + len(partially_correct)) / float(len(merged_records)),
       "weighted satisfied ratio of all txs:", (fully_correct_base_time + partially_correct_base_time) / all_base_time,
       "avg speedup of satisfied txs:",
       avg(fully_correct + partially_correct))
output("")
output("#### Effective Speedup (corresponding to Table 2 in SOSP paper):")
output("")
output("satisfied ratio of all observed txs:",
       (len(fully_correct) + len(partially_correct)) / float(len(merged_records) - len(no_listen)),
       "weighted satisfied ratio of all obersed txs:",
       (fully_correct_base_time + partially_correct_base_time) / (all_base_time - no_listen_base_time),
       "avg speedup of all observed txs:", avg(fully_correct + partially_correct + no_preplay + miss))

output("")
output("#### Breakdown by prediction outcome (corresponding to Table 3 in SOSP paper):")
output("")
output("      Types\t\t\tcount proportion \ttime-weighted proportion\t avg speedup ")
output("1 perfect satisfied\t\t", len(fully_correct) / float(len(merged_records)), "\t",
       fully_correct_base_time / all_base_time, "\t", avg(fully_correct))
output("2 imperfect satisfied\t\t", len(partially_correct) / float(len(merged_records)), "\t",
       partially_correct_base_time / all_base_time, "\t", avg(partially_correct))

output("3 missed\t\t\t", len(wrong) / float(len(merged_records)), "\t", wrong_base_time / all_base_time, "\t",
       avg(wrong))
output("    3.1 predicted but missed\t", len(miss) / float(len(merged_records)), "\t", miss_base_time / all_base_time,
       "\t",
       avg(miss))
output("    3.2 no prediction\t\t", len(no_prediction) / float(len(merged_records)), "\t",
       no_prediction_base_time / all_base_time, "\t", avg(no_prediction))
output("        3.2.1 no observed\t", len(no_listen) / float(len(merged_records)), "\t",
       no_listen_base_time / all_base_time, "\t", avg(no_listen))
output("        3.2.2 no pre-execution\t", len(no_preplay) / float(len(merged_records)), "\t",
       no_preplay_base_time / all_base_time, "\t", avg(no_preplay))

output(" ")
output(" ")

# groups = groupByFullStatus(merged_records)
# gks = sorted(groups.keys())
# for gk in gks:
#     g = groups[gk]
#     output(g[0].fullStatus, len(g) / float(len(merged_records)), avg(g))
#
# output(" ")

output("#### Distribution of speedups:")
output("")
output("\tmax", max_speedup, "p999", p999, "p995", p995, "p99", p99, "p95", p95, ">100", g100)
output("")
output("max line:")
output(max_line)

output("")
output("-----")
output("## Other detail info")
output("")
output("#### Saved IO")
output("")
output("* saved stores percent:", GetSavedStoresPercent(merged_records))
output("* saved loads percent:", GetSavedLoadsPercent(merged_records))
output("* saved ins percent:", GetSavedInsPercent(merged_records))
output(" ")

if len(trace_slowdowns) > 0:
    output("")
    output("-----")
    output("#### Tracer detail")
    output("")
    output("the end-to-end time to pre-execute a transaction in a context and synthesize an AP takes on average ",
           str(total_trace_time / float(total_base_time)) + "x",
           "the time to execute the transaction, without being optimized")
    output("average trace time (milli)",
           total_trace_time / len(trace_slowdowns) / 10 ** 6)
    output("average total slowdown", total_trace_all_time / float(total_base_time), "average total trace time (milli)",
           total_trace_all_time / len(trace_slowdowns) / 10 ** 6, "average trace count",
           total_trace_count * 1.0 / len(trace_slowdowns))
    output("all transaction count", len(merged_lines), "traced transaction count", len(trace_slowdowns))

output(" ")
output(" ")
output(" ")

open(output_filename, "w").writelines(merged_lines)

result_out.close()

