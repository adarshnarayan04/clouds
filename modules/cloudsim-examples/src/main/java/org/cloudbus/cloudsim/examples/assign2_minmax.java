package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

public class assign2_minmax {
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

        // Input tasks
        System.out.print("Enter number of tasks: ");
        int taskCount = scanner.nextInt();
        List<TaskInfo> taskInfos = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            System.out.printf("Length of task %d (MI): ", i);
            long length = scanner.nextLong();
            taskInfos.add(new TaskInfo(i, length));
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

        // Apply Min–Max scheduling
        //Idea: Similar to Min-Min, but instead of picking the task with the global minimum completion time, it picks the task with the maximum completion time among all tasks' minimums
        //This usually favors longer tasks → prevents starvation of big tasks.
        double[] readyTime = new double[vmCount];
        List<TaskInfo> remaining = new ArrayList<>(taskInfos);

        while (!remaining.isEmpty()) {
            TaskInfo chosenTask = null;
            int chosenVm = -1;
            double chosenCT = Double.MIN_VALUE; // track the max of min CTs accross the all VMs

            // compute min CT per task, then pick the max
            for (TaskInfo task : remaining) {
                double minCT = Double.MAX_VALUE;
                int bestVmForTask = -1;
                System.out.printf("Task%-2d|", task.id);

                // find the best VM for this task
                for (VmInfo vm : vmInfos) {
                    double ct = readyTime[vm.id] + ((double) task.length / vm.mips);
                    System.out.printf(" %4.2f |", ct);
                    if (ct < minCT) {
                        minCT = ct;
                        bestVmForTask = vm.id;
                    }
                }
                // among all tasks, select the one with max of minCTs
                //chosenCt - > max of minCTs
                if (minCT > chosenCT) {
                    chosenCT = minCT;
                    chosenTask = task;
                    chosenVm = bestVmForTask;
                }
                System.out.printf("\n");
            }

            // assign the chosen task to its best VM
            chosenTask.cloudlet.setVmId(chosenVm); //the task that is assigned first to the VM , run first in simulation
            readyTime[chosenVm] = chosenCT;
            remaining.remove(chosenTask);

            System.out.printf("Assign task %d → VM%d (CT=%.2f)\n",
                    chosenTask.id, chosenVm, chosenCT);
        }

        broker.submitCloudletList(cloudletList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        // Print results
        List<Cloudlet> result = broker.getCloudletReceivedList();
        printResults(result);
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