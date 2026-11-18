using SPS.Abstractions;

namespace SPS.Student;

// Lab 2 strategy: one file, depends only on SPS.Abstractions.
//
// Main ideas (see README_SOLUTION.md for the reasoning):
//  * Generate jobs bring almost all of the money, so worker capacity follows the queued work and
//    jobs are scheduled by the value they can still earn before the customer's patience runs out.
//  * Metadata/Preview are served from the cache whenever the entry is fresh enough for THIS customer,
//    identical reads are coalesced, and eviction keeps the entries with the best (popularity * price / size).
//  * APIs are cheap but almost idle: keep the minimum, block an API after a burst of failures, probe it back,
//    and start a replacement only when no healthy API remains.
//  * RPC timeouts adapt to the observed latency of each request kind; retries stop when the customer
//    cannot be waiting any more.
public sealed class AdaptiveProfitSystemPolicy : ISystemPolicy
{
    public sealed class Tuning
    {
        // Patience priors. README: Metadata 0.5-1.5 s, Preview 1-3 s, Generate 15-60 s (public 15-35 s).
        // The 45 s parameter shapes Generate priorities. The service can work up to the documented 60 s limit.
        public double MetadataPatienceMax { get; init; } = 1.5;
        public double PreviewPatienceMax { get; init; } = 3.0;
        public double GeneratePatienceMin { get; init; } = 15;
        public double GeneratePatienceMax { get; init; } = 45;

        public int MinApis { get; init; } = 1;                 // API work is tiny; one API is enough
        public int MinWorkers { get; init; } = 1;
        // A replacement may reuse a faulty ordinal, so start more than one if capacity allows.
        public int ApiReplacementBurst { get; init; } = 2;
        public double WorkerBacklogHorizon { get; init; } = 6;
        public double WorkerRateHeadroom { get; init; } = 1.0;
        public int ScaleDownPatience { get; init; } = 2;
        public JobOrder Order { get; init; } = JobOrder.CMu;
        public double JobProfitMargin { get; init; } = 0.5;    // run a job on an already paid worker if value >= 0.5 * its cost
        public double CapacityProfitMargin { get; init; } = 1.0; // buy capacity only for jobs that pay for it
        public bool ShutdownAtEnd { get; init; } = true;
        public double JoinAgeLimit { get; init; } = 0.5;
        public bool DeadlineSizing { get; init; } = true;
        public double WorkerDeadlineSlack { get; init; } = 3;
        public double WorkerRoundDown { get; init; } = 0.25;
    }

    public enum JobOrder { CMu, ValueDensity, Fifo, Lifo }

    private const double DeliveryDelay = 0.002; // engine delivers cached values after 2 ms
    private const double GenerateProcessingLimit = 60;
    private const double WorkerProbePeriod = 3;
    private const double ReachableWorkerSlowdownAllowance = 6;

    private readonly Tuning t;
    private readonly AdaptiveRequestPolicy rpcPolicy;
    public IRequestPolicy RpcPolicy => rpcPolicy;

    private SystemSettings? settings;
    private double now;

    // ---- request bookkeeping
    private readonly Dictionary<long, RequestInfo> requests = new();
    private readonly Dictionary<long, long> lastEndpointOfRequest = new();
    private RequestInfo? routingRequest;

    // ---- latency statistics
    private readonly LatencyWindow[] kindLatency = { new(), new(), new() };

    // ---- arrival statistics (exponentially decayed)
    private readonly DecayedRate arrivals = new(5);
    private readonly DecayedRate generateWorkArrivals = new(8);
    private readonly DecayedRate generateRevenueArrivals = new(8);
    private readonly DecayedRate apiCallArrivals = new(5);
    private double lastArrivalAt = -1;
    private readonly HashSet<long> countedGenerate = new();

    // ---- instances
    private readonly Dictionary<long, ApiState> apiStates = new();
    private readonly Dictionary<long, WorkerState> workerStates = new();
    private readonly Dictionary<long, JobRun> runs = new();

    // ---- cache
    private readonly Dictionary<string, DecayedRate> keyPopularity = new(StringComparer.Ordinal);
    private readonly double[] maxAgeSeen = { 0, 0, 0 };
    private readonly decimal[] revenueSum = { 0, 0, 0 };
    private readonly long[] revenueCount = { 0, 0, 0 };

    // ---- capacity
    private int scaleDownVotes;

    public AdaptiveProfitSystemPolicy() : this(new Tuning()) { }

    public AdaptiveProfitSystemPolicy(Tuning tuning)
    {
        t = tuning;
        rpcPolicy = new AdaptiveRequestPolicy(this);
    }

    // =====================================================================================
    // ISystemPolicy entry points. A bug in an unusual situation must never abort the whole run, so every
    // decision falls back to a simple always-valid answer if the strategy code throws.
    // =====================================================================================
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        try { return GetProcessingBudgetCore(request); } catch { return null; }
    }

    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        try { return OnCacheReadCore(context); }
        catch { routingRequest = context.Request; return CacheReadDecision.StartFetch(); }
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        try { return OnCacheFillCore(context); } catch { return CacheWriteDecision.Skip(); }
    }

    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        try { return SelectEndpointCore(context, random); }
        catch
        {
            routingRequest = context.Request;
            return context.Endpoints.Count == 0 ? null : EndpointSelectionDecision.Assign(context.Endpoints[0].Id);
        }
    }

    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        try { return ReviewEndpointCore(context); } catch { return EndpointDecision.Allow(); }
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        try { return SelectJobAssignmentCore(context); }
        catch
        {
            return context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0 ? null
                : JobAssignmentDecision.Assign(context.QueuedJobs[0].Id, context.AvailableWorkers[0].Id);
        }
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        try { return ReviewJobCore(context); } catch { return JobRecoveryDecision.KeepWaiting(); }
    }

    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        try { return ReviewCapacityCore(context); }
        catch
        {
            var s = context.Settings;
            int apis = Math.Clamp(context.Instances.Count(i => i.Role == InstanceRole.Api && i.State is InstanceState.Ready or InstanceState.Starting), 1, s.MaximumApis);
            int workers = Math.Clamp(context.Instances.Count(i => i.Role == InstanceRole.Worker && i.State is InstanceState.Ready or InstanceState.Starting), 1, s.MaximumWorkers);
            while (apis * s.Api.ComputeUnits + workers * s.Worker.ComputeUnits > s.ComputeUnitLimit && workers > 0) workers--;
            return new CapacityPlan(apis, workers);
        }
    }

    public void Observe(SystemObservation observation)
    {
        try { ObserveCore(observation); } catch { /* observations only refine estimates */ }
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================
    private static double S(TimeSpan x) => x.TotalSeconds;

    private double PatienceMax(RequestKind kind) => kind switch
    {
        RequestKind.Metadata => t.MetadataPatienceMax,
        RequestKind.Preview => t.PreviewPatienceMax,
        _ => GenerateProcessingLimit
    };

    // Conservative estimate for prioritizing work, with a discounted tail up to the documented maximum.
    private double GenerateSurvival(double age)
    {
        if (age >= GenerateProcessingLimit) return 0;
        if (age <= t.GeneratePatienceMin) return 1;
        double conservative = Math.Max(0, (t.GeneratePatienceMax - age) / (t.GeneratePatienceMax - t.GeneratePatienceMin));
        double tail = Math.Max(0, (GenerateProcessingLimit - age) / (GenerateProcessingLimit - t.GeneratePatienceMin));
        return Math.Max(conservative, 0.5 * tail);
    }

    private void Remember(RequestInfo info)
    {
        if (requests.TryAdd(info.Id, info))
        {
            int k = (int)info.Kind;
            if (info.Kind != RequestKind.Generate && S(info.MaxDataAge) > maxAgeSeen[k]) maxAgeSeen[k] = S(info.MaxDataAge);
            revenueSum[k] += info.Revenue;
            revenueCount[k]++;
        }
    }

    private double AverageRevenue(RequestKind kind)
    {
        int k = (int)kind;
        return revenueCount[k] == 0 ? 0.1 : (double)(revenueSum[k] / revenueCount[k]);
    }

    private double ExpectedLatency(RequestKind kind) => kindLatency[(int)kind].Quantile(0.5, kind switch
    {
        RequestKind.Metadata => 0.05,
        RequestKind.Preview => 0.11,
        _ => 0.05
    });

    // =====================================================================================
    // Processing budget: after the longest documented patience nobody pays, stop the work.
    // =====================================================================================
    private TimeSpan? GetProcessingBudgetCore(RequestInfo request)
    {
        Remember(request);
        return TimeSpan.FromSeconds(PatienceMax(request.Kind) + 0.05);
    }

    // =====================================================================================
    // Cache
    // =====================================================================================
    private CacheReadDecision OnCacheReadCore(CacheReadContext context)
    {
        now = S(context.Now);
        var request = context.Request;
        Remember(request);
        Popularity(request.ResourceKey).Add(now, 1);

        var entry = context.Entry;
        if (entry != null && now + DeliveryDelay - S(entry.SourceObservedAt) <= S(request.MaxDataAge) - 0.0005)
            return CacheReadDecision.ServeCached();

        // A pending fetch may have already read the source. Limit how old a shared fetch can be
        // for this caller; the simulator still checks the actual source timestamp at delivery.
        FetchInfo? best = null;
        foreach (var fetch in context.PendingFetches)
            if (best == null || fetch.StartedAt > best.StartedAt) best = fetch;
        if (best != null)
        {
            double age = now - S(best.StartedAt);
            double stuck = t.JoinAgeLimit * PatienceMax(request.Kind);
            double freshnessWindow = S(request.MaxDataAge) - DeliveryDelay;
            if (age <= Math.Min(stuck, freshnessWindow)) return CacheReadDecision.JoinFetch(best.Id);
        }
        routingRequest = request;
        return CacheReadDecision.StartFetch();
    }

    private CacheWriteDecision OnCacheFillCore(CacheFillContext context)
    {
        now = S(context.Now);
        var value = context.Value;
        var kind = KindOfKey(value.Key);
        double maxAge = maxAgeSeen[(int)kind] > 0 ? maxAgeSeen[(int)kind] : (kind == RequestKind.Metadata ? 2 : 5);
        double life = maxAge - (now - S(value.SourceObservedAt)) - DeliveryDelay;
        if (life <= 0.01) return CacheWriteDecision.Skip();

        double incomingScore = EntryScore(value.Key, kind, life, value.SizeBytes);
        long used = 0;
        var candidates = new List<(CacheEntryInfo e, double score)>();
        foreach (var e in context.Entries)
        {
            if (e.Key == value.Key) continue; // replaced in place
            used += e.SizeBytes;
            var ek = KindOfKey(e.Key);
            double emax = maxAgeSeen[(int)ek] > 0 ? maxAgeSeen[(int)ek] : (ek == RequestKind.Metadata ? 2 : 5);
            double eLife = Math.Min(S(e.ExpiresAt) - now, emax - (now - S(e.SourceObservedAt)) - DeliveryDelay);
            candidates.Add((e, eLife <= 0 ? -1 : EntryScore(e.Key, ek, eLife, e.SizeBytes)));
        }
        long free = context.CapacityBytes - used;
        var evict = new List<string>();
        if (free < value.SizeBytes)
        {
            foreach (var (e, score) in candidates.OrderBy(c => c.score))
            {
                if (free >= value.SizeBytes) break;
                if (score >= incomingScore) break;
                evict.Add(e.Key);
                free += e.SizeBytes;
            }
            if (free < value.SizeBytes) return CacheWriteDecision.Skip();
        }
        return CacheWriteDecision.Retain(TimeSpan.FromSeconds(life), evict.ToArray());
    }

    // Expected revenue protected by keeping the entry, per byte.
    private double EntryScore(string key, RequestKind kind, double life, long size)
    {
        double rate = keyPopularity.TryGetValue(key, out var p) ? p.Rate(now) : 0;
        double horizon = Math.Min(life, 3);
        return rate * horizon * AverageRevenue(kind) / Math.Max(1, size);
    }

    private DecayedRate Popularity(string key)
    {
        if (!keyPopularity.TryGetValue(key, out var p)) keyPopularity[key] = p = new DecayedRate(6);
        return p;
    }

    private static RequestKind KindOfKey(string key) =>
        key.StartsWith("Preview", StringComparison.Ordinal) ? RequestKind.Preview :
        key.StartsWith("Generate", StringComparison.Ordinal) ? RequestKind.Generate : RequestKind.Metadata;

    // =====================================================================================
    // API routing and health
    // =====================================================================================
    private EndpointSelectionDecision? SelectEndpointCore(RoutingContext context, Random random)
    {
        now = S(context.Now);
        var request = context.Request;
        Remember(request);
        if (request.Kind == RequestKind.Generate && countedGenerate.Add(request.Id))
        {
            // Only work that pays for its worker time is a reason to buy capacity.
            double price = settings == null ? 0.3 : (double)settings.Worker.PricePerSecond;
            double slotPrice = price / (settings?.Worker.Slots ?? 1);
            if ((double)request.Revenue >= slotPrice * request.EstimatedWorkSeconds * t.CapacityProfitMargin)
            {
                generateWorkArrivals.Add(now, request.EstimatedWorkSeconds);
                generateRevenueArrivals.Add(now, (double)request.Revenue);
            }
        }
        if (context.Attempt == 1) apiCallArrivals.Add(now, 1);
        if (context.Endpoints.Count == 0) return null;

        lastEndpointOfRequest.TryGetValue(request.Id, out long previous);
        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        foreach (var e in context.Endpoints)
        {
            var st = Api(e.Id);
            double load = (e.OutstandingCalls + 1.0) / Math.Max(1, e.Slots);
            double score = (0.5 + load) * st.LatencyEwma / Math.Max(0.1, e.Speed) * (1 + 3 * st.FailureEwma);
            if (context.Attempt > 1 && e.Id == previous && context.Endpoints.Count > 1) score *= 4;
            score *= 1 + 0.01 * random.NextDouble();
            if (score < bestScore) { bestScore = score; best = e; }
        }
        routingRequest = request;
        return EndpointSelectionDecision.Assign(best!.Id);
    }

    private EndpointDecision ReviewEndpointCore(EndpointContext context)
    {
        now = S(context.Now);
        var e = context.Endpoint;
        return e.Role == InstanceRole.Api ? ReviewApi(context, e) : ReviewWorker(context, e);
    }

    private EndpointDecision ReviewApi(EndpointContext context, EndpointInfo e)
    {
        var st = Api(e.Id);
        var results = context.RecentResults;
        if (e.Mode == EndpointMode.AllowTraffic)
        {
            int consecutive = 0, recent = 0, failed = 0;
            for (int i = results.Count - 1; i >= 0; i--)
            {
                var r = results[i];
                if (r.IsProbe || S(r.Time) < st.AllowedSince) continue;
                if (now - S(r.Time) > 2) break;
                bool bad = r.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable;
                recent++;
                if (bad) failed++;
                if (recent == consecutive + 1 && bad) consecutive++;
            }
            if (consecutive >= 3 || recent >= 6 && failed >= 0.6 * recent)
            {
                st.BlockedSince = now;
                return EndpointDecision.Probe();
            }
            return EndpointDecision.Allow();
        }
        // Blocked or probing: reopen after a successful probe that happened after blocking.
        for (int i = results.Count - 1; i >= 0; i--)
        {
            var r = results[i];
            if (S(r.Time) < st.BlockedSince) break;
            if (r.IsProbe && r.Outcome == RequestOutcome.Success)
            {
                st.AllowedSince = now;
                st.FailureEwma *= 0.5;
                return EndpointDecision.Allow();
            }
        }
        return EndpointDecision.Probe();
    }

    private EndpointDecision ReviewWorker(EndpointContext context, EndpointInfo e)
    {
        var st = Worker(e.Id);
        double lastSign = Math.Max(st.ReadyAt, e.LastHeartbeat.HasValue ? S(e.LastHeartbeat.Value) : st.ReadyAt);
        double silence = now - lastSign;
        double hb = settings == null ? 0.5 : S(settings.HeartbeatInterval);
        // Remember the most recent probe that proves the worker is reachable (busy workers answer Rejected).
        foreach (var r in context.RecentResults)
            if (r.IsProbe && r.Outcome is RequestOutcome.Success or RequestOutcome.Rejected)
            {
                st.AliveConfirmedAt = Math.Max(st.AliveConfirmedAt, S(r.Time));
                if (r.Outcome == RequestOutcome.Rejected)
                    st.LastBusyProbeAt = Math.Max(st.LastBusyProbeAt, S(r.Time));
            }
        if (silence <= 3 * hb + 0.1) return EndpointDecision.Allow();
        if (now - st.AliveConfirmedAt < WorkerProbePeriod) return EndpointDecision.Allow(); // heartbeats lost, worker alive
        return EndpointDecision.Probe();
    }

    // =====================================================================================
    // Jobs
    // =====================================================================================
    private JobAssignmentDecision? SelectJobAssignmentCore(JobSchedulingContext context)
    {
        now = S(context.Now);
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        // Prefer workers that are not still physically busy with an abandoned/obsolete execution.
        EndpointInfo? worker = null;
        double bestSpeed = -1;
        foreach (var w in context.AvailableWorkers)
        {
            var ws = Worker(w.Id);
            if (ws.PhysicallyBusyUntil > now) continue;
            double sp = EffectiveSpeed(w.Id, w.Speed);
            if (sp > bestSpeed) { bestSpeed = sp; worker = w; }
        }
        if (worker == null) return null;

        double slots = Math.Max(1, workerStates.Values.Count(w => w.Ready) * (settings?.Worker.Slots ?? 1));
        double backlog = 0;
        foreach (var j in context.QueuedJobs) backlog += j.Request.EstimatedWorkSeconds;
        double expectedWait = backlog / slots;
        double slotPrice = settings == null ? 0.3 : (double)settings.Worker.PricePerSecond / settings.Worker.Slots;

        JobInfo? bestJob = null;
        double bestScore = double.MinValue;
        foreach (var job in context.QueuedJobs)
        {
            var r = job.Request;
            Remember(r);
            double work = r.EstimatedWorkSeconds / bestSpeed;
            double age = now - S(r.CreatedAt);
            double finishAge = age + work + 0.25;
            double survival = GenerateSurvival(finishAge);
            double value = (double)r.Revenue * survival;
            double cost = slotPrice * work;
            if (survival <= 0 || value < cost * t.JobProfitMargin)
                return JobAssignmentDecision.Reject(job.Id); // cannot pay back the worker time

            double score = t.Order switch
            {
                JobOrder.Fifo => -S(r.CreatedAt),
                JobOrder.Lifo => S(r.CreatedAt),
                JobOrder.ValueDensity => value / work,
                _ => CMuScore(r, finishAge, work, expectedWait)
            };
            if (score > bestScore) { bestScore = score; bestJob = job; }
        }
        if (bestJob == null) return null;
        return JobAssignmentDecision.Assign(bestJob.Id, worker.Id);
    }

    // c-mu rule: run first the job whose delay would destroy the most expected revenue per second of work.
    private double CMuScore(RequestInfo r, double finishAge, double work, double expectedWait)
    {
        double rev = (double)r.Revenue;
        double sNow = GenerateSurvival(finishAge);
        double sLater = GenerateSurvival(finishAge + Math.Max(work, expectedWait));
        double loss = rev * (sNow - sLater);
        return (loss + 0.05 * rev * sNow) / work;
    }

    private JobRecoveryDecision ReviewJobCore(JobRecoveryContext context)
    {
        now = S(context.Now);
        var job = context.Job;
        if (job.WorkerId is not long workerId || job.AssignedAt is not TimeSpan assignedAtTs)
            return JobRecoveryDecision.KeepWaiting();
        var ws = Worker(workerId);
        double assignedAt = S(assignedAtTs);
        double hb = settings == null ? 0.5 : S(settings.HeartbeatInterval);
        double speed = EffectiveSpeed(workerId, ws.Speed);
        double retryAt = assignedAt + job.Request.EstimatedWorkSeconds * 1.34 / speed + 1.4;
        // Give the next periodic probe time to return before treating a previously reachable worker as lost.
        double probeGrace = settings == null ? 0.5 : S(settings.ReviewInterval) + S(settings.ProbeTimeout);
        double reachabilityWindow = WorkerProbePeriod + probeGrace;
        bool reachable = now - ws.AliveConfirmedAt <= reachabilityWindow;
        // A busy single-slot worker may be computing the assigned job much longer than estimated.
        int slots = settings?.Worker.Slots ?? 1;
        bool stillBusy = reachable && slots == 1 &&
            ws.LastBusyProbeAt > assignedAt && ws.LastBusyProbeAt >= ws.AliveConfirmedAt;

        // A probe can succeed while other slots are still computing. With no heartbeat to
        // distinguish a lost dispatch from slow progress, allow a bounded slowdown window.
        if (slots > 1 && reachable && ws.AliveConfirmedAt > assignedAt && now - ws.LastHeartbeat > 3 * hb + 0.1)
        {
            double nominalWork = job.Request.EstimatedWorkSeconds / Math.Max(0.05, ws.Speed);
            double slowFinish = assignedAt + nominalWork * ReachableWorkerSlowdownAllowance + 1.4;
            double latestRetry = S(job.Request.CreatedAt) + GenerateProcessingLimit - nominalWork - 0.25;
            retryAt = Math.Max(retryAt, Math.Min(slowFinish, latestRetry));
        }

        if (job.LastHeartbeat == null)
        {
            // The worker keeps reporting but never mentions this job: the dispatch was lost or rejected.
            if (ws.LastHeartbeat > assignedAt + 0.08 && now - assignedAt > hb + 0.1)
            {
                ws.PhysicallyBusyUntil = Math.Max(ws.PhysicallyBusyUntil, now + 0.5);
                return Requeue(job, crashed: false);
            }
            if (now - assignedAt > 3 * hb + 0.3 && !reachable) return Requeue(job, crashed: true);
            if (!stillBusy && now >= retryAt) return Requeue(job, crashed: false);
            return JobRecoveryDecision.KeepWaiting();
        }

        double silence = now - S(job.LastHeartbeat.Value);
        if (silence <= 3 * hb + 0.1) return JobRecoveryDecision.KeepWaiting();
        // Heartbeats are lost but probes reach the worker: it is probably still computing.
        if (reachable && (stillBusy || now < retryAt)) return JobRecoveryDecision.KeepWaiting();
        return Requeue(job, crashed: !reachable);
    }

    private JobRecoveryDecision Requeue(JobInfo job, bool crashed)
    {
        if (job.WorkerId is long wid && runs.TryGetValue(job.Id, out var run) && !crashed)
        {
            var ws = Worker(wid);
            ws.PhysicallyBusyUntil = Math.Max(ws.PhysicallyBusyUntil, Math.Min(run.ExpectedEnd, now + 6));
        }
        return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
    }

    private double EffectiveSpeed(long workerId, double nominal)
    {
        var ws = Worker(workerId);
        return Math.Max(0.05, nominal / (ws.SlowRatio > 1.6 ? ws.SlowRatio : 1));
    }

    // =====================================================================================
    // Capacity
    // =====================================================================================
    private CapacityPlan ReviewCapacityCore(CapacityContext context)
    {
        now = S(context.Now);
        settings ??= context.Settings;
        var s = context.Settings;
        var apis = context.Instances.Where(i => i.Role == InstanceRole.Api).ToList();
        var workers = context.Instances.Where(i => i.Role == InstanceRole.Worker).ToList();
        foreach (var w in workers)
        {
            var ws = Worker(w.Id);
            ws.Speed = w.Speed;
            ws.Ready = w.State == InstanceState.Ready;
        }

        bool idle = t.ShutdownAtEnd && lastArrivalAt >= 0 &&
            now - lastArrivalAt > Math.Max(3, 10.0 / Math.Max(0.5, arrivals.PeakRate));

        // ---------------- APIs
        var apiActive = apis.Where(a => a.State is InstanceState.Ready or InstanceState.Starting).ToList();
        int healthy = apis.Count(a => a.State == InstanceState.Ready && a.Mode == EndpointMode.AllowTraffic);
        int startingApis = apis.Count(a => a.State == InstanceState.Starting);
        double apiWork = apiCallArrivals.Rate(now) * 0.03 * 3; // generous: ~15 ms of API work per call
        int apiTarget = Math.Max(t.MinApis, (int)Math.Ceiling(apiWork / Math.Max(1, s.Api.Slots * s.Api.Speed)));
        var retire = new List<long>();
        double apiColdStart = S(s.CapacityReviewInterval) + S(s.Api.StartupDelay) +
            S(s.ReviewInterval) + ExpectedLatency(RequestKind.Preview);
        bool apiCanRestartInTime = apiColdStart < t.PreviewPatienceMax;
        if (idle && apiCanRestartInTime && context.PendingCalls == 0) apiTarget = 0;
        else if (healthy == 0 && startingApis == 0)
            apiTarget = Math.Max(apiTarget, apiActive.Count + t.ApiReplacementBurst); // all APIs sick: add fresh ones
        else if (healthy < apiTarget && startingApis == 0)
            apiTarget = Math.Max(apiTarget, apiActive.Count + 1);
        else
        {
            // Enough healthy APIs: retire the ones that are blocked for a while.
            foreach (var a in apiActive)
            {
                var st = Api(a.Id);
                if (a.State == InstanceState.Ready && a.Mode != EndpointMode.AllowTraffic && now - st.BlockedSince > 2 &&
                    apiActive.Count - retire.Count > apiTarget)
                    retire.Add(a.Id);
            }
        }
        apiTarget = Math.Min(apiTarget, s.MaximumApis);
        int apiKeep = apiActive.Count - retire.Count;
        int apiDesired = Math.Max(apiTarget, 0);
        if (apiDesired < apiKeep)
        {
            // scale down: drop blocked first, then the slowest idle ones
            foreach (var a in apiActive.Where(a => !retire.Contains(a.Id))
                         .OrderBy(a => a.State == InstanceState.Ready && a.Mode == EndpointMode.AllowTraffic ? 1 : 0)
                         .ThenBy(a => a.OutstandingCalls).ThenBy(a => a.Speed))
            {
                if (apiActive.Count - retire.Count <= apiDesired) break;
                retire.Add(a.Id);
            }
        }

        // ---------------- workers
        int workerSlots = s.Worker.Slots;
        double price = (double)s.Worker.PricePerSecond / workerSlots;
        double avgSpeed = workers.Count == 0 ? s.Worker.Speed : workers.Average(w => EffectiveSpeed(w.Id, w.Speed));
        avgSpeed = Math.Max(0.3, avgSpeed);
        // Jobs that are still worth running, with a soft deadline: finish before patience may start to run out.
        var pending = new List<(double deadline, double work)>();
        double queuedWork = 0;
        foreach (var j in context.QueuedJobs)
        {
            double w = j.Request.EstimatedWorkSeconds / avgSpeed;
            double age = now - S(j.Request.CreatedAt);
            if (GenerateSurvival(age + w) * (double)j.Request.Revenue <= price * w * t.CapacityProfitMargin) continue;
            queuedWork += w;
            pending.Add((Math.Max(S(j.Request.CreatedAt) + t.GeneratePatienceMin - t.WorkerDeadlineSlack, now + w), w));
        }
        double runningWork = 0;
        foreach (var r in runs.Values)
        {
            double rem = Math.Max(0.2, r.Estimate / Math.Max(0.3, r.Speed) - (now - r.AssignedAt));
            runningWork += rem;
            pending.Add((now + rem, rem));
        }
        double rate = generateWorkArrivals.Rate(now) / avgSpeed;
        double density = rate > 1e-9 ? generateRevenueArrivals.Rate(now) / (rate * avgSpeed) : 1;
        if (density < price * 1.05 || idle) rate = 0; // nothing new to prepare for
        double needed;
        if (t.DeadlineSizing)
        {
            // Estimate how many parallel slots the soft deadlines and the incoming workload require.
            pending.Sort((a, b) => a.deadline.CompareTo(b.deadline));
            double cumulative = 0, kDeadline = 0;
            foreach (var (deadline, work) in pending)
            {
                cumulative += work;
                kDeadline = Math.Max(kDeadline, cumulative / Math.Max(0.5, deadline - now));
            }
            needed = Math.Max(kDeadline, rate * t.WorkerRateHeadroom);
        }
        else needed = rate * t.WorkerRateHeadroom + (queuedWork + runningWork) / t.WorkerBacklogHorizon;
        int workerTarget = (int)Math.Ceiling(needed / workerSlots - t.WorkerRoundDown);
        bool anyWork = queuedWork > 0 || runs.Count > 0;
        workerTarget = Math.Max(workerTarget, idle && !anyWork ? 0 : (idle ? 1 : t.MinWorkers));

        int cuLeft = s.ComputeUnitLimit - Math.Max(apiDesired, apiKeep) * s.Api.ComputeUnits;
        int workerCap = Math.Min(s.MaximumWorkers, Math.Max(0, cuLeft / s.Worker.ComputeUnits));
        workerTarget = Math.Min(workerTarget, workerCap);

        var workerActive = workers.Where(w => w.State is InstanceState.Ready or InstanceState.Starting).ToList();
        // A blocked worker still consumes capacity. Keep it probed and add a replacement
        // when queued work has nowhere to run and the budget has room for another worker.
        if (queuedWork > 0 && workerActive.Count > 0 &&
            !workerActive.Any(w => w.State == InstanceState.Ready && w.Mode == EndpointMode.AllowTraffic))
        {
            bool replacementStarting = workerActive.Any(w => w.State == InstanceState.Starting);
            int replacementTarget = workerActive.Count + (replacementStarting ? 0 : 1);
            workerTarget = Math.Max(workerTarget, Math.Min(workerCap, replacementTarget));
        }

        int workerDesired = workerActive.Count;
        if (workerTarget > workerActive.Count) { workerDesired = workerTarget; scaleDownVotes = 0; }
        else if (workerTarget < workerActive.Count)
        {
            scaleDownVotes++;
            bool idleExists = workerActive.Any(w => w.AssignedJobs == 0 && Worker(w.Id).PhysicallyBusyUntil <= now);
            if ((scaleDownVotes >= t.ScaleDownPatience || idle) && idleExists)
            {
                int drop = idle ? workerActive.Count - workerTarget : 1;
                var victims = workerActive.Where(w => w.AssignedJobs == 0 && Worker(w.Id).PhysicallyBusyUntil <= now)
                    .OrderBy(w => w.State == InstanceState.Starting ? 0 : 1)
                    .ThenBy(w => w.Mode == EndpointMode.AllowTraffic ? 1 : 0)
                    .ThenBy(w => EffectiveSpeed(w.Id, w.Speed)).Take(drop).ToList();
                foreach (var v in victims) retire.Add(v.Id);
                workerDesired = workerActive.Count - victims.Count;
                scaleDownVotes = 0;
            }
        }
        else scaleDownVotes = 0;

        int apiFinal = apiActive.Count(a => !retire.Contains(a.Id));
        if (apiDesired > apiFinal) apiFinal = apiDesired;
        // Respect the shared compute budget.
        while (apiFinal * s.Api.ComputeUnits + workerDesired * s.Worker.ComputeUnits > s.ComputeUnitLimit && workerDesired > 0)
            workerDesired--;
        while (apiFinal * s.Api.ComputeUnits + workerDesired * s.Worker.ComputeUnits > s.ComputeUnitLimit && apiFinal > 1)
            apiFinal--;
        apiFinal = Math.Min(apiFinal, s.MaximumApis);
        workerDesired = Math.Min(workerDesired, s.MaximumWorkers);
        return new CapacityPlan(apiFinal, workerDesired, retire.Count == 0 ? null : retire);
    }

    // =====================================================================================
    // Observations
    // =====================================================================================
    private void ObserveCore(SystemObservation o)
    {
        now = S(o.Now);
        switch (o.Kind)
        {
            case ObservationKind.SimulationStarted:
                settings = o.Settings;
                break;
            case ObservationKind.RequestArrived:
                arrivals.Add(now, 1);
                lastArrivalAt = now;
                break;
            case ObservationKind.RequestSucceeded:
            case ObservationKind.RequestFailed:
                if (o.RequestId is long done)
                {
                    requests.Remove(done);
                    lastEndpointOfRequest.Remove(done);
                    countedGenerate.Remove(done);
                    if (runs.Remove(done, out var run) && o.Kind == ObservationKind.RequestFailed)
                    {
                        // The engine stops tracking the job but the worker may still be computing it.
                        var ws = Worker(run.WorkerId);
                        ws.PhysicallyBusyUntil = Math.Max(ws.PhysicallyBusyUntil, Math.Min(run.ExpectedEnd, now + 6));
                    }
                }
                break;
            case ObservationKind.RpcAttemptStarted:
                if (o.RequestId is long rid && o.EndpointId is long eid) lastEndpointOfRequest[rid] = eid;
                break;
            case ObservationKind.RpcAttemptCompleted:
                if (o.EndpointId is long apiId && o.Elapsed is TimeSpan el)
                {
                    var st = Api(apiId);
                    bool ok = o.Outcome == RequestOutcome.Success;
                    st.FailureEwma = st.FailureEwma * 0.8 + (ok ? 0 : 0.2);
                    if (ok) st.LatencyEwma = st.LatencyEwma * 0.8 + 0.2 * S(el);
                }
                break;
            case ObservationKind.WorkerHeartbeatReceived:
                if (o.EndpointId is long hw) Worker(hw).LastHeartbeat = now;
                break;
            case ObservationKind.JobAssigned:
                if (o.JobId is long jid && o.EndpointId is long w)
                {
                    double est = requests.TryGetValue(o.RequestId ?? jid, out var info) ? info.EstimatedWorkSeconds : 3;
                    var ws = Worker(w);
                    double speed = EffectiveSpeed(w, ws.Speed);
                    runs[jid] = new JobRun(w, now, est, now + est / speed + 0.2, speed);
                }
                break;
            case ObservationKind.JobRequeued:
                if (o.JobId is long rq) runs.Remove(rq);
                break;
            case ObservationKind.JobCompleted:
                if (o.JobId is long cj && runs.Remove(cj, out var finished) && o.EndpointId is long cw && cw == finished.WorkerId)
                {
                    var ws = Worker(cw);
                    double nominal = finished.Estimate / Math.Max(0.05, ws.Speed);
                    double ratio = (now - finished.AssignedAt - 0.15) / Math.Max(0.2, nominal);
                    ws.SlowRatio = ws.SlowRatio * 0.5 + 0.5 * Math.Clamp(ratio, 0.5, 10);
                }
                break;
            case ObservationKind.InstanceReady:
                if (o.EndpointId is long ready)
                {
                    if (workerStates.TryGetValue(ready, out var wr)) { wr.ReadyAt = now; wr.Ready = true; }
                    else if (!apiStates.ContainsKey(ready)) pendingReady[ready] = now;
                }
                break;
            case ObservationKind.InstanceStopped:
            case ObservationKind.InstanceDraining:
                if (o.EndpointId is long gone && workerStates.TryGetValue(gone, out var wg)) wg.Ready = false;
                break;
        }
    }

    private readonly Dictionary<long, double> pendingReady = new();

    private ApiState Api(long id)
    {
        if (!apiStates.TryGetValue(id, out var st))
        {
            apiStates[id] = st = new ApiState { LatencyEwma = 0.1 };
            pendingReady.Remove(id);
        }
        return st;
    }

    private WorkerState Worker(long id)
    {
        if (!workerStates.TryGetValue(id, out var st))
        {
            workerStates[id] = st = new WorkerState { Speed = 1, Ready = true };
            if (pendingReady.Remove(id, out var at)) st.ReadyAt = at;
            else st.ReadyAt = now;
        }
        return st;
    }

    // =====================================================================================
    // RPC policy (lab 1 part): adaptive timeouts per request kind, bounded retries.
    // =====================================================================================
    private sealed class AdaptiveRequestPolicy : IRequestPolicy
    {
        private readonly AdaptiveProfitSystemPolicy owner;
        private readonly Dictionary<long, RpcState> rpcs = new();
        // Multiplicative timeout correction: grows on timeouts (congestion), decays on successes.
        private readonly double[] scale = { 1, 1, 1 };

        public AdaptiveRequestPolicy(AdaptiveProfitSystemPolicy owner) => this.owner = owner;

        public double TimeoutFor(RequestKind kind, int attempt)
        {
            var lat = owner.kindLatency[(int)kind];
            double q = lat.Quantile(0.97, double.NaN);
            double timeout = kind switch
            {
                RequestKind.Metadata => double.IsNaN(q) ? 0.12 : Math.Clamp(q * 1.5 + 0.015, 0.06, 0.4),
                RequestKind.Preview => double.IsNaN(q) ? 0.25 : Math.Clamp(q * 1.5 + 0.03, 0.14, 0.9),
                _ => double.IsNaN(q) ? 0.3 : Math.Clamp(q * 2 + 0.05, 0.15, 1.5)
            };
            timeout *= scale[(int)kind];
            return timeout * (attempt <= 1 ? 1 : Math.Min(2, 1 + 0.25 * (attempt - 1)));
        }

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            try { return GetTimeoutCore(context); } catch { return TimeSpan.FromMilliseconds(250); }
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            try { return OnResultCore(observation, random); } catch { return RetryDecision.NoRetry(); }
        }

        private TimeSpan GetTimeoutCore(RequestContext context)
        {
            if (!rpcs.TryGetValue(context.RequestId, out var st))
            {
                var info = owner.routingRequest;
                st = new RpcState(info?.Kind ?? RequestKind.Metadata, info?.CreatedAt.TotalSeconds ?? owner.now);
                rpcs[context.RequestId] = st;
                if (rpcs.Count > 20000) Cleanup();
            }
            st.AttemptStart = context.Attempt == 1 ? owner.now : Math.Max(owner.now, st.NextStart);
            double timeout = TimeoutFor(st.Kind, context.Attempt);
            return TimeSpan.FromSeconds(timeout);
        }

        private RetryDecision OnResultCore(RequestObservation observation, Random random)
        {
            if (!rpcs.TryGetValue(observation.RequestId, out var st)) return RetryDecision.NoRetry();
            double elapsed = observation.Elapsed.TotalSeconds;
            double at = st.AttemptStart + elapsed;
            if (observation.Outcome == RequestOutcome.Success)
            {
                owner.kindLatency[(int)st.Kind].Add(elapsed);
                scale[(int)st.Kind] = Math.Max(1, scale[(int)st.Kind] * 0.97);
                rpcs.Remove(observation.RequestId);
                return RetryDecision.NoRetry();
            }
            if (observation.Outcome == RequestOutcome.Timeout) scale[(int)st.Kind] = Math.Min(8, scale[(int)st.Kind] * 1.25);
            int maxAttempts = st.Kind == RequestKind.Generate ? 20 : 5;
            double age = at - st.CreatedAt;
            double delay = observation.Outcome switch
            {
                RequestOutcome.Rejected => (st.Kind == RequestKind.Generate ? 0.2 : 0.01) * observation.Attempt * (0.5 + random.NextDouble()),
                RequestOutcome.Unavailable => 0.002,
                _ => 0.001 * random.NextDouble()
            };
            double expected = owner.ExpectedLatency(st.Kind);
            if (observation.Attempt >= maxAttempts || age + delay + expected > owner.PatienceMax(st.Kind))
            {
                rpcs.Remove(observation.RequestId);
                return RetryDecision.NoRetry();
            }
            st.NextStart = at + delay;
            return RetryDecision.RetryAfter(TimeSpan.FromSeconds(delay));
        }

        private void Cleanup()
        {
            double cutoff = owner.now - 120;
            foreach (var key in rpcs.Where(p => p.Value.AttemptStart < cutoff).Select(p => p.Key).ToList()) rpcs.Remove(key);
        }

        private sealed class RpcState
        {
            public readonly RequestKind Kind;
            public readonly double CreatedAt;
            public double AttemptStart, NextStart;
            public RpcState(RequestKind kind, double createdAt) { Kind = kind; CreatedAt = createdAt; }
        }
    }

    // =====================================================================================
    // Small state classes
    // =====================================================================================
    private sealed class ApiState
    {
        public double LatencyEwma;
        public double FailureEwma;
        public double BlockedSince = double.NegativeInfinity;
        public double AllowedSince = double.NegativeInfinity;
    }

    private sealed class WorkerState
    {
        public double Speed = 1;
        public bool Ready;
        public double ReadyAt;
        public double LastHeartbeat = double.NegativeInfinity;
        public double AliveConfirmedAt = double.NegativeInfinity;
        public double LastBusyProbeAt = double.NegativeInfinity;
        public double PhysicallyBusyUntil = double.NegativeInfinity;
        public double SlowRatio = 1;
    }

    private sealed record JobRun(long WorkerId, double AssignedAt, double Estimate, double ExpectedEnd, double Speed);

    // Exponentially decayed event rate (events per second) with time constant tau.
    private sealed class DecayedRate
    {
        private readonly double tau;
        private double value, at;
        public double PeakRate { get; private set; }
        public DecayedRate(double tau) { this.tau = tau; }
        public void Add(double time, double amount)
        {
            Decay(time);
            value += amount;
            PeakRate = Math.Max(PeakRate, value / tau);
        }
        public double Rate(double time) { Decay(time); return value / tau; }
        private void Decay(double time)
        {
            if (time > at) { value *= Math.Exp(-(time - at) / tau); at = time; }
        }
    }

    // Sliding window of recent successful latencies with a cached quantile.
    private sealed class LatencyWindow
    {
        private readonly double[] data = new double[256];
        private int count, next, sinceSort;
        private double[] sorted = Array.Empty<double>();
        public void Add(double v)
        {
            data[next] = v;
            next = (next + 1) % data.Length;
            if (count < data.Length) count++;
            sinceSort++;
        }
        public double Quantile(double q, double fallback)
        {
            if (count < 20) return fallback;
            if (sinceSort > 16 || sorted.Length != count)
            {
                sorted = data.Take(count).ToArray();
                Array.Sort(sorted);
                sinceSort = 0;
            }
            return sorted[Math.Min(count - 1, (int)(q * count))];
        }
    }
}
