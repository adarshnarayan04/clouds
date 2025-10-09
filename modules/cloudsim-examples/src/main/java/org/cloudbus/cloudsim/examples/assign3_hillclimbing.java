package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.*;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

public class assign3_hillclimbing {

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
        }

        // --- Hill Climbing Scheduling Algorithm ---
        System.out.println("\n--- Starting Hill Climbing Scheduling ---");

        // 1. Generate an initial random schedule
        int[] currentSchedule = new int[taskCount];
        for (int i = 0; i < taskCount; i++) {
            currentSchedule[i] = random.nextInt(vmCount);
        }
        System.out.println("Initial random schedule: " + Arrays.toString(currentSchedule));

        double currentMakespan = calculateMakespan(currentSchedule, taskInfos, vmInfos);
        System.out.printf("Initial Makespan: %.2f seconds\n", currentMakespan);

        boolean improved = true;
        while (improved) {
            improved = false;
            int bestTaskToMove = -1;
            int bestVmDestination = -1;
            double bestMakespan = currentMakespan;

            //  Explore neighbors by moving one task at a time
            for (int taskId = 0; taskId < taskCount; taskId++) {
                for (int vmId = 0; vmId < vmCount; vmId++) {
                    // Skip if the task is already on this VM
                    if (currentSchedule[taskId] == vmId) {
                        continue;
                    }

                    // Create a temporary "neighbor" schedule with the move
                    int[] neighborSchedule = Arrays.copyOf(currentSchedule, taskCount);
                    neighborSchedule[taskId] = vmId;

                    // Evaluate the neighbor
                    double neighborMakespan = calculateMakespan(neighborSchedule, taskInfos, vmInfos);
                    System.out.printf("  > Move Task %d to VM %d: Potential Makespan = %.2f\n", taskId, vmId, neighborMakespan);

                    // If this move is the best one found so far, record it
                    if (neighborMakespan < bestMakespan) {
                        bestMakespan = neighborMakespan;
                        bestTaskToMove = taskId;
                        bestVmDestination = vmId;
                    }
                }
            }

            // If a better schedule was found, adopt it and continue the loop
            if (bestTaskToMove != -1) {
                System.out.printf("Improvement found: Move Task %d to VM %d. New Makespan: %.2f\n",
                        bestTaskToMove, bestVmDestination, bestMakespan);
                currentSchedule[bestTaskToMove] = bestVmDestination; //updating the current schedule
                currentMakespan = bestMakespan;
                improved = true; // Set to true to continue the outer while loop
            }
        } // If no improvement was found after checking all moves, the loop terminates.

        System.out.println("--- Hill Climbing Finished ---");
        System.out.println("Final optimized schedule: " + Arrays.toString(currentSchedule));
        System.out.printf("Final Optimized Makespan: %.2f seconds\n\n", currentMakespan);

        // Apply the final schedule to the cloudlets
        for (int i = 0; i < taskCount; i++) {
            cloudletList.get(i).setVmId(currentSchedule[i]);
        }
        // --- End of Hill Climbing Section ---

        broker.submitCloudletList(cloudletList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();

        // Print results
        List<Cloudlet> result = broker.getCloudletReceivedList();
        printResults(result);
    }

    /**
     * Calculates the makespan for a given schedule.
     * The makespan is the maximum finish time among all VMs.
     */
    private static double calculateMakespan(int[] schedule, List<TaskInfo> tasks, List<VmInfo> vms) {
        double[] vmFinishTimes = new double[vms.size()];

        // Calculate the total execution time for each VM
        for (int taskId = 0; taskId < schedule.length; taskId++) {
            int vmId = schedule[taskId];
            TaskInfo task = tasks.get(taskId);
            VmInfo vm = vms.get(vmId);

            double executionTime = (double) task.length / vm.mips;
            vmFinishTimes[vmId] += executionTime;
        }

        // Find the maximum finish time (the makespan)
        double makespan = 0.0;
        for (double finishTime : vmFinishTimes) {
            if (finishTime > makespan) {
                makespan = finishTime;
            }
        }
        return makespan;
    }


    private static void printResults(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        System.out.println();
        System.out.println("========== SIMULATION RESULTS ==========");
        System.out.println("Number of Cloudlets: " + size);
        System.out.println();

        System.out.println("┌─────────┬─────────┬──────────────┬──────────────┬──────────────┬──────────────┬───────────────┐");
        System.out.println("│ TASK ID │  VM ID  │ START TIME   │ FINISH TIME  │   CPU TIME   │   WAIT TIME  │ TURNAROUND    │");
        System.out.println("├─────────┼─────────┼──────────────┼──────────────┼──────────────┼──────────────┼───────────────┤");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            double start = cloudlet.getExecStartTime();
            double finish = cloudlet.getFinishTime();
            double wait = start - cloudlet.getSubmissionTime();
            double turnaround = finish - cloudlet.getSubmissionTime();

            System.out.printf("│ %-7d │ %-7d │ %-12s │ %-12s │ %-12s │ %-12s │ %-13s │\n",
                    cloudlet.getCloudletId(),
                    cloudlet.getVmId(),
                    dft.format(start),
                    dft.format(finish),
                    dft.format(cloudlet.getActualCPUTime()),
                    dft.format(wait),
                    dft.format(turnaround));
        }
        System.out.println("└─────────┴─────────┴──────────────┴──────────────┴──────────────┴──────────────┴───────────────┘");

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
            System.out.println("Makespan: " + dft.format(maxFinish)); // This is the actual makespan from the simulation
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

    // Helper class to store VM info
    static class VmInfo {
        int id, mips;
        VmInfo(int id, int mips) { this.id = id; this.mips = mips; }
    }

    // Helper class to store Task info
    static class TaskInfo {
        int id;
        long length;
        TaskInfo(int id, long length) { this.id = id; this.length = length; }
    }
}