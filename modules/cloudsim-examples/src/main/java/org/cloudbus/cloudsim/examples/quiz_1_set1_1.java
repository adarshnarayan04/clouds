package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

public class quiz_1_set1_1 {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        Random random = new Random();

        // Input VMs
        System.out.print("Enter number of VMs (>=2): ");
        int vmCount = Math.max(2, scanner.nextInt());
        List<VmInfo> vmInfos = new ArrayList<>();
        for (int i = 0; i < vmCount; i++) {
            int mips = 100 + random.nextInt(1501);
            vmInfos.add(new VmInfo(i, mips));
            System.out.printf("VM%d MIPS=%d\n", i, mips);
        }

        // Input tasks with deadlines
        System.out.print("Enter number of tasks: ");
        int taskCount = scanner.nextInt();
        List<TaskInfo> taskInfos = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            System.out.printf("Length of task %d (MI): ", i);
            long length = scanner.nextLong();
            System.out.printf("Deadline for task %d (time units): ", i);
            double deadline = scanner.nextDouble();
            taskInfos.add(new TaskInfo(i, length, deadline));
        }

        // Initialize CloudSim
        CloudSim.init(1, Calendar.getInstance(), false);
        Datacenter dc = createDatacenter("DC");
        DatacenterBroker broker = new DatacenterBroker("Broker");

        List<Vm> vmList = new ArrayList<>();
        for (VmInfo info : vmInfos) {
            Vm vm = new Vm(info.id, broker.getId(), info.mips, 1, 512, 1000, 10000, "Xen",
                    new CloudletSchedulerSpaceShared());
            vmList.add(vm);
        }
        broker.submitGuestList(vmList);

        List<Cloudlet> cloudletList = new ArrayList<>();
        UtilizationModel um = new UtilizationModelFull();
        for (TaskInfo t : taskInfos) {
            Cloudlet cl = new Cloudlet(t.id, t.length, 1, 300, 300, um, um, um);
            cl.setUserId(broker.getId());
            cloudletList.add(cl);
            t.cloudlet = cl;
        }

        // Apply Deadline-aware Min-Min scheduling
        System.out.println("\n=== DEADLINE-AWARE MIN-MIN SCHEDULING ===");
        System.out.println("Priority: Earliest deadline first, then minimum completion time");

        double[] readyTime = new double[vmCount];
        List<TaskInfo> remaining = new ArrayList<>(taskInfos);
        List<SchedulingDecision> decisions = new ArrayList<>();

        while (!remaining.isEmpty()) {
            // Sort by deadline first (EDF principle)
            remaining.sort(Comparator.comparingDouble(t -> t.deadline));

            System.out.println("\nRemaining tasks sorted by deadline:");
            for (TaskInfo t : remaining) {
                System.out.printf("Task%d (length=%d, deadline=%.2f) ", t.id, t.length, t.deadline);
            }
            System.out.println();

            double bestCT = Double.MAX_VALUE;
            TaskInfo bestTask = null;
            int bestVm = -1;

            // Consider tasks in deadline order, but still use Min-Min for tie-breaking
            for (TaskInfo task : remaining) {
                System.out.printf("Task%-2d (DL=%.2f)|", task.id, task.deadline);
                for (VmInfo vm : vmInfos) {
                    double ct = readyTime[vm.id] + ((double) task.length / vm.mips);
                    System.out.printf(" VM%d:%.2f |", vm.id, ct);

                    // Priority: earliest deadline, then minimum completion time
                    if (bestTask == null ||
                            task.deadline < bestTask.deadline ||
                            (task.deadline == bestTask.deadline && ct < bestCT)) {
                        bestCT = ct;
                        bestTask = task;
                        bestVm = vm.id;
                    }
                }
                System.out.println();
            }

            bestTask.cloudlet.setVmId(bestVm);
            readyTime[bestVm] = bestCT;
            decisions.add(new SchedulingDecision(bestTask, bestVm, bestCT));
            remaining.remove(bestTask);

            boolean willMissDeadline = bestCT > bestTask.deadline;
            System.out.printf("→ Assign Task%d → VM%d (CT=%.2f, DL=%.2f) %s\n",
                    bestTask.id, bestVm, bestCT, bestTask.deadline,
                    willMissDeadline ? "[DEADLINE MISS!]" : "[OK]");
        }

        broker.submitCloudletList(cloudletList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        // Print results
        List<Cloudlet> result = broker.getCloudletReceivedList();
        printResults(result, taskInfos, decisions);
    }

    private static void printResults(List<Cloudlet> list, List<TaskInfo> taskInfos, List<SchedulingDecision> decisions) {
        System.out.println("\n========== SIMULATION RESULTS ==========");
        System.out.println("Deadline-Aware Min-Min Scheduling Results");
        System.out.println("Number of Cloudlets: " + list.size());
        System.out.println();

        System.out.println("┌──────┬─────┬───────────┬───────────┬───────────┬──────────┬────────────┬──────────────┐");
        System.out.println("│ TASK │ VM  │   START   │  FINISH   │    CPU    │ DEADLINE │ TURNAROUND │ DEADLINE HIT │");
        System.out.println("├──────┼─────┼───────────┼───────────┼───────────┼──────────┼────────────┼──────────────┤");

        DecimalFormat dft = new DecimalFormat("###.##");
        int missedDeadlines = 0;
        double totalTurnaround = 0;
        double totalWait = 0;
        double maxFinish = 0;

        for (Cloudlet cloudlet : list) {
            double start = cloudlet.getExecStartTime();
            double finish = cloudlet.getFinishTime();
            double wait = start - cloudlet.getSubmissionTime();
            double turnaround = finish - cloudlet.getSubmissionTime();

            // Find deadline for this task
            double deadline = 0;
            for (TaskInfo t : taskInfos) {
                if (t.id == cloudlet.getCloudletId()) {
                    deadline = t.deadline;
                    break;
                }
            }

            boolean missedDeadline = finish > deadline;
            if (missedDeadline) missedDeadlines++;

            totalTurnaround += turnaround;
            totalWait += wait;
            if (finish > maxFinish) maxFinish = finish;

            System.out.printf("│ %-4d │ %-3d │ %-9s │ %-9s │ %-9s │ %-8s │ %-10s │ %-12s │\n",
                    cloudlet.getCloudletId(),
                    cloudlet.getVmId(),
                    dft.format(start),
                    dft.format(finish),
                    dft.format(cloudlet.getActualCPUTime()),
                    dft.format(deadline),
                    dft.format(turnaround),
                    missedDeadline ? "MISSED" : "MET");
        }
        System.out.println("└──────┴─────┴───────────┴───────────┴───────────┴──────────┴────────────┴──────────────┘");

        // Print comprehensive statistics
        System.out.println("\nPERFORMANCE METRICS");
        System.out.println("-------------------");
        System.out.printf("Total Tasks: %d\n", list.size());
        System.out.printf("Deadlines Met: %d\n", list.size() - missedDeadlines);
        System.out.printf("Deadlines Missed: %d\n", missedDeadlines);
        System.out.printf("Deadline Miss Rate: %.2f%%\n", (double)missedDeadlines / list.size() * 100);
        System.out.printf("Average Turnaround Time: %s\n", dft.format(totalTurnaround / list.size()));
        System.out.printf("Average Wait Time: %s\n", dft.format(totalWait / list.size()));
        System.out.printf("Makespan: %s\n", dft.format(maxFinish));

        // Show scheduling decisions summary
        System.out.println("\nSCHEDULING DECISIONS SUMMARY");
        System.out.println("----------------------------");
        for (SchedulingDecision decision : decisions) {
            TaskInfo task = decision.task;
            boolean predicted = decision.completionTime > task.deadline;
            boolean actual = false;
            for (Cloudlet c : list) {
                if (c.getCloudletId() == task.id) {
                    actual = c.getFinishTime() > task.deadline;
                    break;
                }
            }
            System.out.printf("Task%d → VM%d: Predicted %s, Actual %s\n",
                    task.id, decision.vmId,
                    predicted ? "MISS" : "MEET",
                    actual ? "MISS" : "MEET");
        }

        System.out.println("\nAlgorithm: Deadline-Aware Min-Min Scheduling");
        System.out.println("Strategy: EDF priority with Min-Min completion time optimization");
        System.out.println("========================================");
    }

    private static Datacenter createDatacenter(String name) throws Exception {
        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = Collections.singletonList(new Pe(0, new PeProvisionerSimple(1000000)));
        hostList.add(new Host(0, new RamProvisionerSimple(2048000), new BwProvisionerSimple(1000000),
                100000000, peList, new VmSchedulerTimeShared(peList)));

        DatacenterCharacteristics ch = new DatacenterCharacteristics(
                "x86", "Linux", "Xen", hostList, 0.0, 3.0, 0.05, 0.001, 0);
        return new Datacenter(name, ch, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
    }

    static class VmInfo {
        int id, mips;
        VmInfo(int id, int mips) { this.id = id; this.mips = mips; }
    }

    static class TaskInfo {
        int id;
        long length;
        double deadline;
        Cloudlet cloudlet;

        TaskInfo(int id, long length, double deadline) {
            this.id = id;
            this.length = length;
            this.deadline = deadline;
        }
    }

    static class SchedulingDecision {
        TaskInfo task;
        int vmId;
        double completionTime;

        SchedulingDecision(TaskInfo task, int vmId, double completionTime) {
            this.task = task;
            this.vmId = vmId;
            this.completionTime = completionTime;
        }
    }
}
