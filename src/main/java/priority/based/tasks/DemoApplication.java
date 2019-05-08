package priority.based.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

@SpringBootApplication
public class DemoApplication {

    @Autowired
    private TaskExecutor taskExecutor;
    @Autowired
    private JobBusiness jobBusiness;

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner() {

        return args -> {
            Task initialFirst = new Task(jobBusiness::performSomethingOn, new Job(1L, 1L));
            Task initialSecond = new Task(jobBusiness::performSomethingOn, new Job(2L, 2L));
            // Core pool size is reached, start appending jobs to the queue
            Task waitingInQueueFirst = new Task(jobBusiness::performSomethingOn, new Job(3L, 5L));
            Task waitingInQueueSecond = new Task(jobBusiness::performSomethingOn, new Job(4L, 1L));
            Task waitingInQueueThird = new Task(jobBusiness::performSomethingOn, new Job(5L, 10L));
            taskExecutor.execute(new FutureCustomTask(initialFirst));
            taskExecutor.execute(new FutureCustomTask(initialSecond));
            /*
             Once initial jobs are finished, `waitingInQueueSecond` Job will get the first freed thread
             since it's priority is higher than priorities of `waitingInQueueFirst` and `waitingInQueueThird`
            */
            taskExecutor.execute(new FutureCustomTask(waitingInQueueFirst));
            taskExecutor.execute(new FutureCustomTask(waitingInQueueSecond));
            taskExecutor.execute(new FutureCustomTask(waitingInQueueThird));
            /* Expected output:
            Received Job with 1 ID of priority 1
                Received Job with 2 ID of priority 2
                    Sleeping for 10 seconds
                    Sleeping for 10 seconds
                Finished Job with 2 ID of priority 2
                Finished Job with 1 ID of priority 1

                Received Job with 4 ID of priority 1 (waitingInQueueSecond got the first freed thread)
                Received Job with 3 ID of priority 5
                    Sleeping for 10 seconds
                    Sleeping for 10 seconds
                Finished Job with 4 ID of priority 1
                Finished Job with 3 ID of priority 5

                Received Job with 5 ID of priority 10
                    Sleeping for 10 seconds
                Finished Job with 5 ID of priority 10
             */
        };
    }

    @Bean("CustomTaskExecutor")
    public TaskExecutor threadPoolTaskExecutor(
            @Value("${spring.async.core-pool-size}") int corePoolSize,
            @Value("${spring.async.max-pool-size}") int maxPoolSize,
            @Value("${spring.async.queue-capacity}") int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor() {

            @Override
            protected BlockingQueue<Runnable> createQueue(int queueCapacity) {
                return new PriorityBlockingQueue<>(queueCapacity);
            }

        };
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        return executor;
    }

    public static class Job {
        private Long id;
        private Long priority;

        public Job(Long id, Long priority) {
            this.id = id;
            this.priority = priority;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getPriority() {
            return priority;
        }

        public void setPriority(Long priority) {
            this.priority = priority;
        }
    }

    public static class Task implements Runnable {
        private Consumer<Job> jobConsumer;
        private Job job;

        public Task(Consumer<Job> jobConsumer, Job job) {
            this.jobConsumer = jobConsumer;
            this.job = job;
        }

        public Job getJob() {
            return this.job;
        }

        @Override
        public void run() {
            this.jobConsumer.accept(job);
        }
    }

    public static class FutureCustomTask extends FutureTask<FutureCustomTask> implements Comparable<FutureCustomTask> {
        private Task task;

        public FutureCustomTask(Task task) {
            super(task, null);
            this.task = task;
        }

        @Override
        public int compareTo(FutureCustomTask o) {
            return task.getJob().getPriority().compareTo(o.task.getJob().getPriority());
        }
    }

    @Component
    public static class JobBusiness {

        Logger logger = LoggerFactory.getLogger(getClass());

        public void performSomethingOn(Job job) {
            logger.info("Received Job with {} ID of priority {}", job.getId(), job.getPriority());
            logger.info("Sleeping for 10 seconds");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Finished Job with {} ID of priority {}", job.getId(), job.getPriority());
        }
    }

}
