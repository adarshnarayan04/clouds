package org.cloudbus.cloudsim.examples;
import java.util.Scanner;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;


public class perf_rr2 {
    public static DatacenterBroker broker;
    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmlist;

    static class CloudletInfo {
        int id;
        long burstTime;
        long remainingTime;
        Cloudlet cloudlet;
        double startTime;
        double finishTime;
        int vmId;

        CloudletInfo(int id, long burstTime, Cloudlet cloudlet) {
            this.id = id;
            this.burstTime = burstTime;
            this.remainingTime = burstTime;
            this.cloudlet = cloudlet;
            this.startTime = -1;
            this.finishTime = -1;
            this.vmId = -1;
        }
    }

    static class ExecutionRecord {
        int step;
        int cloudletId;
        int vmId;
        double startTime;
        double endTime;
        long executedQuantum;
        long remainingTime;
        String status;

        ExecutionRecord(int step, int cloudletId, int vmId, double startTime, double endTime,
                       long executedQuantum, long remainingTime, String status) {
            this.step = step;
            this.cloudletId = cloudletId;
            this.vmId = vmId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.executedQuantum = executedQuantum;
            this.remainingTime = remainingTime;
            this.status = status;
        }
    }

    public static void main(String[] args) {
        System.out.println("Round Robin Scheduler with Time Shared Policy");
        System.out.println("=".repeat(50));

        try {
            CloudSim.init(1, Calendar.getInstance(), false);
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            broker = new DatacenterBroker("Broker");
            int brokerId = broker.getId();

            // Create 2 VMs with 1000 MIPS each using TimeShared scheduler
            vmlist = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Vm vm = new Vm(i, brokerId, 1000, 1, 512, 1000, 10000, "Xen",
                              new CloudletSchedulerTimeShared());
                vmlist.add(vm);
            }
            broker.submitGuestList(vmlist);


            long[] burstTimes = {2000, 3000, 4000, 5000, 6000, 7000};
            long timeQuantumMI = 10;

            System.out.println("Configuration:");
            System.out.println("Number of VMs: 2 (1000 MIPS each)");
            System.out.println("Time Quantum: 10 ms (" + timeQuantumMI + " MI)");
            System.out.println("Cloudlet Burst Times (MI): ");
            for (int i = 0; i < burstTimes.length; i++) {
                System.out.println("  Cloudlet " + i + ": " + burstTimes[i] + " MI");
            }
            System.out.println();

            cloudletList = new ArrayList<>();
            List<CloudletInfo> cloudletInfoList = new ArrayList<>();
            UtilizationModel utilizationModel = new UtilizationModelFull();

            for (int i = 0; i < burstTimes.length; i++) {
                Cloudlet cloudlet = new Cloudlet(i, burstTimes[i], 1, 300, 300,
                                               utilizationModel, utilizationModel, utilizationModel);
                cloudlet.setUserId(brokerId);
                cloudletList.add(cloudlet);
                cloudletInfoList.add(new CloudletInfo(i, burstTimes[i], cloudlet));
            }


            List<ExecutionRecord> executionSequence = simulateRoundRobinScheduling(cloudletInfoList, timeQuantumMI);

            // Assign cloudlets to VMs based on round robin results
            for (CloudletInfo info : cloudletInfoList) {
                info.cloudlet.setVmId(info.vmId);
            }

            broker.submitCloudletList(cloudletList);
            CloudSim.startSimulation();
            CloudSim.stopSimulation();

            List<Cloudlet> results = broker.getCloudletReceivedList();

            // Print execution order table
            printExecutionOrderTable(executionSequence);

            // Print results and statistics
            printResults(results);
            calculateStatistics(results);

            // Analyze time quantum effect
            analyzeTimeQuantumEffect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<ExecutionRecord> simulateRoundRobinScheduling(List<CloudletInfo> cloudletInfoList, long timeQuantum) {
        System.out.println("Round Robin Scheduling Simulation:");
        System.out.println("-".repeat(50));

        Queue<CloudletInfo> readyQueue = new LinkedList<>();
        List<ExecutionRecord> executionSequence = new ArrayList<>();

        // Initialize all cloudlets in ready queue
        for (CloudletInfo info : cloudletInfoList) {
            readyQueue.offer(info);
        }

        // Track ready time for each VM separately
        double[] vmReadyTime = new double[2];
        int step = 1;

        while (!readyQueue.isEmpty()) {
            CloudletInfo current = readyQueue.poll();

            // Find the VM that will be ready earliest
            int selectedVm = 0;
            for (int i = 1; i < vmReadyTime.length; i++) {
                if (vmReadyTime[i] < vmReadyTime[selectedVm]) {
                    selectedVm = i;
                }
            }

            // Assign to selected VM
            current.vmId = selectedVm;
            double startTime = vmReadyTime[selectedVm];

            if (current.startTime == -1) {
                current.startTime = startTime;
            }

            // Calculate execution time for this quantum
            long executionQuantum = Math.min(timeQuantum, current.remainingTime);
            double actualExecutionTime = (double) executionQuantum / 1000.0; // Convert MI to seconds (1000 MIPS)

            // Create execution record
            String status;
            if (current.remainingTime <= timeQuantum) {
                status = "COMPLETED";
                current.finishTime = startTime + actualExecutionTime;
            } else {
                status = "PREEMPTED";
            }

            ExecutionRecord record = new ExecutionRecord(
                step++, current.id, selectedVm, startTime,
                startTime + actualExecutionTime, executionQuantum,
                current.remainingTime - executionQuantum, status
            );
            executionSequence.add(record);

            // Update current cloudlet
            current.remainingTime -= executionQuantum;

            // Update VM ready time
            vmReadyTime[selectedVm] = startTime + actualExecutionTime;

            if (current.remainingTime > 0) {
                // Not finished, add back to queue
                readyQueue.offer(current);
            }
        }

        return executionSequence;
    }

    private static void printExecutionOrderTable(List<ExecutionRecord> executionSequence) {
        System.out.println("EXECUTION ORDER TABLE:");
        System.out.println("=".repeat(90));
        System.out.printf("%-6s %-10s %-6s %-12s %-12s %-12s %-12s %-10s\n",
                         "Step", "Cloudlet", "VM", "Start Time", "End Time", "Quantum", "Remaining", "Status");
        System.out.println("-".repeat(90));

        DecimalFormat df = new DecimalFormat("##.###");
        for (ExecutionRecord record : executionSequence) {
            System.out.printf("%-6d %-10d %-6d %-12s %-12s %-12d %-12d %-10s\n",
                             record.step,
                             record.cloudletId,
                             record.vmId,
                             df.format(record.startTime),
                             df.format(record.endTime),
                             record.executedQuantum,
                             record.remainingTime,
                             record.status);
        }
        System.out.println("=".repeat(90));
        System.out.println();
    }

    private static void printResults(List<Cloudlet> list) {
        System.out.println("FINAL CLOUDLET EXECUTION RESULTS:");
        System.out.println("=".repeat(80));
        System.out.printf("%-12s %-12s %-8s %-12s %-12s %-12s\n",
                         "Cloudlet ID", "Length (MI)", "VM ID", "Start Time", "Finish Time", "Exec Time");
        System.out.println("-".repeat(80));

        DecimalFormat dft = new DecimalFormat("##.###");
        for (Cloudlet cloudlet : list) {
            if (cloudlet.getStatus() == Cloudlet.CloudletStatus.SUCCESS) {
                System.out.printf("%-12d %-12d %-8d %-12s %-12s %-12s\n",
                                 cloudlet.getCloudletId(),
                                 cloudlet.getCloudletLength(),
                                 cloudlet.getGuestId(),
                                 dft.format(cloudlet.getExecStartTime()),
                                 dft.format(cloudlet.getExecFinishTime()),
                                 dft.format(cloudlet.getActualCPUTime()));
            }
        }
        System.out.println("=".repeat(80));
        System.out.println();
    }

    private static void calculateStatistics(List<Cloudlet> list) {
        System.out.println("PERFORMANCE METRICS:");
        System.out.println("=".repeat(60));

        double totalWaitingTime = 0;
        double totalTurnaroundTime = 0;
        double totalExecutionTime = 0;
        double makespan = 0;
        int completedCloudlets = 0;

        DecimalFormat dft = new DecimalFormat("##.###");

        System.out.printf("%-12s %-12s %-15s %-15s\n", "Cloudlet ID", "Exec Time", "Waiting Time", "Turnaround Time");
        System.out.println("-".repeat(60));

        for (Cloudlet cloudlet : list) {
            if (cloudlet.getStatus() == Cloudlet.CloudletStatus.SUCCESS) {
                double executionTime = cloudlet.getActualCPUTime();
                double waitingTime = cloudlet.getExecStartTime() - cloudlet.getSubmissionTime();
                double turnaroundTime = cloudlet.getExecFinishTime() - cloudlet.getSubmissionTime();
                double finishTime = cloudlet.getExecFinishTime();

                System.out.printf("%-12d %-12s %-15s %-15s\n",
                                 cloudlet.getCloudletId(),
                                 dft.format(executionTime),
                                 dft.format(waitingTime),
                                 dft.format(turnaroundTime));

                totalExecutionTime += executionTime;
                totalWaitingTime += waitingTime;
                totalTurnaroundTime += turnaroundTime;

                // Calculate makespan (maximum finish time)
                if (finishTime > makespan) {
                    makespan = finishTime;
                }

                completedCloudlets++;
            }
        }

        System.out.println("-".repeat(60));
        System.out.println("AVERAGE METRICS:");
        System.out.println("Average Execution Time: " + dft.format(totalExecutionTime / completedCloudlets) + " seconds");
        System.out.println("Average Waiting Time: " + dft.format(totalWaitingTime / completedCloudlets) + " seconds");
        System.out.println("Average Turnaround Time: " + dft.format(totalTurnaroundTime / completedCloudlets) + " seconds");
        System.out.println("Total Execution Time: " + dft.format(totalExecutionTime) + " seconds");
        System.out.println("Makespan: " + dft.format(makespan) + " seconds");
        System.out.println("=".repeat(60));
    }

    private static void analyzeTimeQuantumEffect() {
        System.out.println("\nTIME QUANTUM ANALYSIS:");
        System.out.println("=".repeat(50));
        System.out.println("Current Time Quantum: 10 ms");
        System.out.println();
        System.out.println("Effects of Time Quantum Variation:");
        System.out.println("• Smaller quantum (1-5 ms):");
        System.out.println("  - More context switches and overhead");
        System.out.println("  - Better response time for interactive tasks");
        System.out.println("  - Higher CPU utilization overhead");
        System.out.println();
        System.out.println("• Current quantum (10 ms):");
        System.out.println("  - Balanced performance");
        System.out.println("  - Reasonable context switch frequency");
        System.out.println();
        System.out.println("• Larger quantum (50+ ms):");
        System.out.println("  - Fewer context switches, less overhead");
        System.out.println("  - Approaches FCFS behavior");
        System.out.println("  - May increase waiting time for short tasks");
        System.out.println("=".repeat(50));
    }

    private static Datacenter createDatacenter(String name) {

        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();

        int mips = 10000;
        peList.add(new Pe(0, new PeProvisionerSimple(mips)));

        int hostId = 0;
        int ram = 2048;
        long storage = 1000000;
        int bw = 10000;

        hostList.add(
                new Host(
                        hostId,
                        new RamProvisionerSimple(ram),
                        new BwProvisionerSimple(bw),
                        storage,
                        peList,
                        new VmSchedulerTimeShared(peList)
                )
        );

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.001;
        double costPerBw = 0.0;
        LinkedList<Storage> storageList = new LinkedList<>();

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    private static void printCloudletList(List<Cloudlet> list) {
        System.out.println();
        System.out.println("Execution Results");
        System.out.println("Task  Status     DC  VM   CPU    Start   Finish   Wait    TAT");
        System.out.println("----  ------     --  --  -----   -----   ------   ----   ----");

        DecimalFormat dft = new DecimalFormat("##.##");
        for (Cloudlet cloudlet : list) {
            if (cloudlet.getStatus() == Cloudlet.CloudletStatus.SUCCESS) {
                System.out.print(String.format("%4d", cloudlet.getCloudletId()));
                System.out.print("  SUCCESS");
                System.out.print(String.format("%8d", cloudlet.getResourceId()));
                System.out.print(String.format("%8d", cloudlet.getGuestId()));
                System.out.print(String.format("%8.2f", cloudlet.getActualCPUTime()));
                System.out.print(String.format("%8.2f", cloudlet.getExecStartTime()));
                System.out.print(String.format("%8.2f", cloudlet.getExecFinishTime()));
                System.out.print(String.format("%8.2f", cloudlet.getExecStartTime() - cloudlet.getSubmissionTime()));
                System.out.print(String.format("%8.2f", cloudlet.getExecFinishTime() - cloudlet.getSubmissionTime()));
                System.out.println();
            }
        }
    }
}
