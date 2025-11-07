package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

public class perf_minmin {
    public static void main(String[] args) throws Exception {

        List<VmInfo> vmInfos = Arrays.asList(
            new VmInfo(0, 500),
            new VmInfo(1, 1000),
                new VmInfo(2, 1500)

        );


        List<TaskInfo> taskInfos = Arrays.asList(
            new TaskInfo(0, 2000),
            new TaskInfo(1, 4000),
            new TaskInfo(2, 8000),
            new TaskInfo(3, 10000)

        );

        System.out.println("=== VM Configuration ===");
        for (VmInfo vm : vmInfos) {
            System.out.printf("VM%d MIPS=%d\n", vm.id, vm.mips);
        }

        System.out.println("\n=== Task Configuration ===");
        for (TaskInfo task : taskInfos) {
            System.out.printf("Task%d Length=%d MI\n", task.id, task.length);
        }


        computeAndDisplayECTMatrix(taskInfos, vmInfos);


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

        // Apply Min–Min scheduling
        System.out.println("\n=== Min-Min Scheduling Process ===");
        double[] readyTime = new double[vmInfos.size()];
        List<TaskInfo> remaining = new ArrayList<>(taskInfos);

        System.out.printf("%-8s", "");
        for (VmInfo vm : vmInfos) {
            System.out.printf("%-8s", "VM" + vm.id);
        }
        System.out.println();

        while (!remaining.isEmpty()) {
            double bestCT = Double.MAX_VALUE;
            TaskInfo bestTask = null;
            int bestVm = -1;
            for (TaskInfo task : remaining) {
                System.out.printf("Task%-3d|", task.id);
                for (VmInfo vm : vmInfos) {
                    double ct = readyTime[vm.id] + ((double) task.length / vm.mips);
                    System.out.printf("%-7.2f|", ct);
                    if (ct < bestCT) {
                        bestCT = ct;
                        bestTask = task;
                        bestVm = vm.id;
                    }
                }
                System.out.printf("\n");
            }
            bestTask.cloudlet.setVmId(bestVm);
            readyTime[bestVm] = bestCT;
            remaining.remove(bestTask);
            System.out.printf("Assign task %d → VM%d (CT=%.2f)\n\n", bestTask.id, bestVm, bestCT);
        }

        broker.submitCloudletList(cloudletList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        // Print results
        List<Cloudlet> result = broker.getCloudletReceivedList();
        printCloudletVMMapping(result, vmInfos);
        printResults(result);
    }

    private static void computeAndDisplayECTMatrix(List<TaskInfo> taskInfos, List<VmInfo> vmInfos) {
        System.out.println("\n=== Expected Completion Time (ECT) Matrix ===");

        // Create ECT matrix
        double[][] ectMatrix = new double[taskInfos.size()][vmInfos.size()];

        // Print header
        System.out.printf("%-8s", "Tasks\\VMs");
        for (VmInfo vm : vmInfos) {
            System.out.printf("%-10s", "VM" + vm.id);
        }
        System.out.println();
        System.out.println("-".repeat(8 + vmInfos.size() * 10));

        // Compute and display ECT matrix
        DecimalFormat df = new DecimalFormat("###.##");
        for (int i = 0; i < taskInfos.size(); i++) {
            TaskInfo task = taskInfos.get(i);
            System.out.printf("Task%-4d", task.id);

            for (int j = 0; j < vmInfos.size(); j++) {
                VmInfo vm = vmInfos.get(j);
                double ect = (double) task.length / vm.mips; // Execution time
                ectMatrix[i][j] = ect;
                System.out.printf("%-10s", df.format(ect));
            }
            System.out.println();
        }
        System.out.println();

        // Display minimum execution time for each task
        System.out.println("=== Minimum Execution Time for Each Task ===");
        for (int i = 0; i < taskInfos.size(); i++) {
            double minTime = Double.MAX_VALUE;
            int bestVM = -1;
            for (int j = 0; j < vmInfos.size(); j++) {
                if (ectMatrix[i][j] < minTime) {
                    minTime = ectMatrix[i][j];
                    bestVM = j;
                }
            }
            System.out.printf("Task%d: %.2f seconds on VM%d\n", i, minTime, bestVM);
        }
        System.out.println();
    }

    private static void printCloudletVMMapping(List<Cloudlet> cloudletList, List<VmInfo> vmInfos) {
        System.out.println("\n" + "=".repeat(90));
        System.out.println("                           CLOUDLET-VM MAPPING TABLE");
        System.out.println("=".repeat(90));
        System.out.printf("%-12s %-15s %-8s %-12s %-15s %-15s %-12s\n",
                         "Cloudlet ID", "Length (MI)", "VM ID", "VM MIPS",
                         "Start Time", "Finish Time", "Exec Time");
        System.out.println("-".repeat(90));

        DecimalFormat dft = new DecimalFormat("###.##");
        double totalExecutionTime = 0;
        double makespan = 0;

        for (Cloudlet cloudlet : cloudletList) {
            int vmId = cloudlet.getVmId();
            VmInfo assignedVm = null;

            // Find the assigned VM
            for (VmInfo vm : vmInfos) {
                if (vm.id == vmId) {
                    assignedVm = vm;
                    break;
                }
            }

            double startTime = cloudlet.getExecStartTime();
            double finishTime = cloudlet.getFinishTime();
            double executionTime = cloudlet.getActualCPUTime();

            totalExecutionTime += executionTime;
            if (finishTime > makespan) {
                makespan = finishTime;
            }

            System.out.printf("%-12d %-15d %-8d %-12d %-15s %-15s %-12s\n",
                             cloudlet.getCloudletId(),
                             cloudlet.getCloudletLength(),
                             vmId,
                             assignedVm != null ? assignedVm.mips : 0,
                             dft.format(startTime),
                             dft.format(finishTime),
                             dft.format(executionTime));
        }

        System.out.println("-".repeat(90));
        System.out.printf("Total Execution Time: %s seconds\n", dft.format(totalExecutionTime));
        System.out.printf("Makespan: %s seconds\n", dft.format(makespan));
        System.out.println("=".repeat(90));
    }

    private static void printResults(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        System.out.println();
        System.out.println("========== SIMULATION RESULTS ==========");
        System.out.println("Number of Cloudlets: " + size);
        System.out.println();

        System.out.println("┌───────┬───────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐");
        System.out.println("│ TASK  │  VM   │    START    │    FINISH   │     CPU     │     WAIT    │ TURNAROUND  │");
        System.out.println("├───────┼───────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┤");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            double start = cloudlet.getExecStartTime();
            double finish = cloudlet.getFinishTime();
            double wait = start - cloudlet.getSubmissionTime();
            double turnaround = finish - cloudlet.getSubmissionTime();

            System.out.printf("│ %-5d │ %-5d │ %-11s │ %-11s │ %-11s │ %-11s │ %-11s │\n",
                    cloudlet.getCloudletId(),
                    cloudlet.getVmId(),
                    dft.format(start),
                    dft.format(finish),
                    dft.format(cloudlet.getActualCPUTime()),
                    dft.format(wait),
                    dft.format(turnaround));
        }
        System.out.println("└───────┴───────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘");

        // Print summary statistics
        if (size > 0) {
            double totalTurnaround = 0;
            double totalWait = 0;
            double maxFinish = 0;

            for (Cloudlet cl : list) {
                double finish = cl.getFinishTime();
                double start = cl.getExecStartTime();
                double wait = start - cl.getSubmissionTime();
                double turnaround = finish - cl.getSubmissionTime();

                totalTurnaround += turnaround;
                totalWait += wait;
                if (finish > maxFinish) {
                    maxFinish = finish;
                }
            }

            System.out.println();
            System.out.println("SUMMARY STATISTICS");
            System.out.println("------------------");
            System.out.println("Average Turnaround Time: " + dft.format(totalTurnaround/size));
            System.out.println("Average Wait Time: " + dft.format(totalWait/size));
            System.out.println("Makespan: " + dft.format(maxFinish));
            System.out.println();
        }

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
        Cloudlet cloudlet;
        TaskInfo(int id, long length) { this.id = id; this.length = length; }
    }
}

