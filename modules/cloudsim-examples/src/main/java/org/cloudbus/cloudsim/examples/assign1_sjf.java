package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
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

/**
 * Assignment 1: CloudSim simulation with 5 cloudlets, 1 broker, 1 VM
 * Using proper NON-PREEMPTIVE SJF (Shortest Job First) scheduling considering both Arrival Time and Burst Time
 */
public class assign1_sjf {
    public static DatacenterBroker broker;
    private static List<Cloudlet> cloudletList;
    private static List<Vm> vmlist;

    static class CloudletInfo {
        int id;
        double arrivalTime;
        long burstTime;
        Cloudlet cloudlet;

        CloudletInfo(int id, double arrivalTime, long burstTime, Cloudlet cloudlet) {
            this.id = id;
            this.arrivalTime = arrivalTime;
            this.burstTime = burstTime;
            this.cloudlet = cloudlet;
        }
    }

    public static void main(String[] args) {
        System.out.println("CloudSim Assignment 1 - SJF Scheduler");
        System.out.println("Non-Preemptive Shortest Job First Algorithm");
        System.out.println();

        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            broker = new DatacenterBroker("Broker");
            int brokerId = broker.getId();
            vmlist = new ArrayList<>();

            int vmid = 0;//indentifer of VM
            int mips = 1000;//millions instructions per  second
            long size = 10000;
            int ram = 512;
            long bw = 1000;//bandwidth (Mbps)
            int pesNumber = 1;//cpu cores
            String vmm = "Xen"; //name of virtual machine

            Vm vm = new Vm(vmid, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            vmlist.add(vm);
            broker.submitGuestList(vmlist);

            cloudletList = new ArrayList<>();
            List<CloudletInfo> cloudletInfoList = new ArrayList<>();

            System.out.println("Initial Cloudlet Configuration:");

            // {Arrival Time, Burst Time}
            double[][] cloudletData = {
                    {0.0, 100000},
                    {2.0, 50000},
                    {1.0, 20000},
                    {3.0, 75000},
                    {4.0, 150000}
            };

            UtilizationModel utilizationModel = new UtilizationModelFull();

            for (int i = 0; i < 5; i++) {
                double arrivalTime = cloudletData[i][0];
                long burstTime = (long) cloudletData[i][1];
                long fileSize = 300;
                long outputSize = 300;

                Cloudlet cloudlet = new Cloudlet(i, burstTime, pesNumber, fileSize,
                        outputSize, utilizationModel, utilizationModel,
                        utilizationModel);//cpu , ram , bandwidth
                cloudlet.setUserId(brokerId);
                cloudlet.setGuestId(vmid);
                cloudlet.setSubmissionTime(arrivalTime);

                cloudletList.add(cloudlet);
                cloudletInfoList.add(new CloudletInfo(i, arrivalTime, burstTime, cloudlet));
            }

            System.out.println();
            printCloudletInputInfo(cloudletInfoList);

            List<CloudletInfo> sjfSchedule = applySJFScheduling(cloudletInfoList);
            //give the cloudlets in the order of SJF scheduling, so that broker can execute them in that order
            // it executes in the order they are submitted (FCFS manner)

            System.out.println();
            printSJFSchedule(sjfSchedule);

            cloudletList.clear();
            for (CloudletInfo info : sjfSchedule) {
                cloudletList.add(info.cloudlet);
            }

            broker.submitCloudletList(cloudletList);
            CloudSim.startSimulation();
            CloudSim.stopSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();
            printCloudletList(newList);
            calculateStatistics(newList);
            System.out.println("\nSimulation completed successfully.");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error occurred during simulation");
        }
    }

    private static List<CloudletInfo> applySJFScheduling(List<CloudletInfo> cloudletInfoList) {
        System.out.println("Applying SJF Scheduling Algorithm...");
        System.out.println("Rule: Select shortest job among all arrived jobs");

        List<CloudletInfo> scheduledJobs = new ArrayList<>();
        List<CloudletInfo> remainingJobs = new ArrayList<>(cloudletInfoList);

        double currentTime = 0.0;

        while (!remainingJobs.isEmpty()) {
            List<CloudletInfo> arrivedJobs = new ArrayList<>();
            for (CloudletInfo job : remainingJobs) {
                if (job.arrivalTime <= currentTime) {
                    arrivedJobs.add(job);
                }
            }

            if (arrivedJobs.isEmpty()) {
                double nextArrival = Double.MAX_VALUE;
                for (CloudletInfo job : remainingJobs) {
                    if (job.arrivalTime < nextArrival) {
                        nextArrival = job.arrivalTime;
                    }
                }

                currentTime = nextArrival;
                continue;
            }

            CloudletInfo selectedJob = Collections.min(arrivedJobs,
                    Comparator.comparingLong(job -> job.burstTime));

            double startTime = currentTime;
            selectedJob.cloudlet.setSubmissionTime(startTime);
            scheduledJobs.add(selectedJob);
            remainingJobs.remove(selectedJob);

            double executionTime = (double) selectedJob.burstTime / 1000.0;
            double finishTime = startTime + executionTime;

            List<CloudletInfo> waitingJobs = new ArrayList<>();
            for (CloudletInfo job : remainingJobs) {
                if (job.arrivalTime <= finishTime) {
                    waitingJobs.add(job);
                }
            }
            currentTime = finishTime;
        }

        return scheduledJobs;
    }

    private static void printCloudletInputInfo(List<CloudletInfo> list) {
        System.out.println("Job Details:");
        System.out.println("Task    AT(s)    BT(MI)    File    Output");
        System.out.println("----    -----    ------    ----    ------");

        for (CloudletInfo info : list) {
            System.out.printf("%4d    %5.1f    %6d    %4d    %6d%n",
                    info.id, info.arrivalTime, info.burstTime,
                    info.cloudlet.getCloudletFileSize(),
                    info.cloudlet.getCloudletOutputSize());
        }
    }

    private static void printSJFSchedule(List<CloudletInfo> sjfSchedule) {
        System.out.println("SJF Execution Sequence:");
        System.out.println("Pos  Task   AT     BT");
        System.out.println("---  ----  ----  ------");

        for (int i = 0; i < sjfSchedule.size(); i++) {
            CloudletInfo info = sjfSchedule.get(i);
            System.out.printf("%3d  %4d  %4.1f  %6d%n",
                    (i+1), info.id, info.arrivalTime, info.burstTime);
        }
    }

    private static Datacenter createDatacenter(String name) {

        List<Host> hostList = new ArrayList<>();
        List<Pe> peList = new ArrayList<>();

        int mips = 1000;
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
                double waitingTime = cloudlet.getExecStartTime() - cloudlet.getSubmissionTime();
                double turnaroundTime = cloudlet.getExecFinishTime() - cloudlet.getSubmissionTime();

                System.out.printf("%4d  %-8s   %2d  %2d  %5s   %5s   %6s   %4s   %4s%n",
                        cloudlet.getCloudletId(),
                        "OK",
                        cloudlet.getResourceId(),
                        cloudlet.getGuestId(),
                        dft.format(cloudlet.getActualCPUTime()),
                        dft.format(cloudlet.getExecStartTime()),
                        dft.format(cloudlet.getExecFinishTime()),
                        dft.format(waitingTime),
                        dft.format(turnaroundTime));
            }
        }
    }

    private static void calculateStatistics(List<Cloudlet> list) {
        System.out.println();
        System.out.println("Performance Metrics");

        double totalWaitingTime = 0;
        double totalTurnaroundTime = 0;
        double totalExecutionTime = 0;

        DecimalFormat dft = new DecimalFormat("##.##");

        for (Cloudlet cloudlet : list) {
            if (cloudlet.getStatus() == Cloudlet.CloudletStatus.SUCCESS) {
                double waitingTime = cloudlet.getExecStartTime() - cloudlet.getSubmissionTime();
                double turnaroundTime = cloudlet.getExecFinishTime() - cloudlet.getSubmissionTime();
                double executionTime = cloudlet.getActualCPUTime();

                totalWaitingTime += waitingTime;
                totalTurnaroundTime += turnaroundTime;
                totalExecutionTime += executionTime;
            }
        }

        int completedCloudlets = list.size();

        System.out.println("Tasks completed: " + completedCloudlets);
        System.out.println("Avg wait time: " + dft.format(totalWaitingTime / completedCloudlets) + "s");
        System.out.println("Avg turnaround: " + dft.format(totalTurnaroundTime / completedCloudlets) + "s");
        System.out.println("Avg execution: " + dft.format(totalExecutionTime / completedCloudlets) + "s");
        System.out.println("Total runtime: " + dft.format(totalExecutionTime) + "s");
        System.out.println("Algorithm: Non-preemptive SJF");
    }
}