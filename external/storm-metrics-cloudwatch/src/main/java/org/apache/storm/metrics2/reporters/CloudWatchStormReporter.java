package org.apache.storm.metrics2.reporters;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.metrics2.filters.StormMetricsFilter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;


public class CloudWatchStormReporter extends ScheduledStormReporter {
    private final static Logger LOG = LoggerFactory.getLogger(CloudWatchStormReporter.class);

    public static final String DEFAULT_NAMESPACE = "storm";

    public static final String CLOUDWATCH_REGION = "cloudwatch.region";
    public static final String CLOUDWATCH_NAMESPACE = "cloudwatch.namespace";

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf, Map reporterConf) {
        LOG.debug("Preparing...");

        final AmazonCloudWatchAsyncClientBuilder cloudWatchBuilder = AmazonCloudWatchAsyncClientBuilder.standard();

        Regions region = getCloudwatchRegion(reporterConf);
        if(region != null) {
            cloudWatchBuilder.withRegion(region);
        } else {
            LOG.info("No AWS region was specified for the CloudWatch Metrics.");
        }

        String namespace = getCloudwatchNamespace(reporterConf);
        if(namespace == null) {
            namespace = DEFAULT_NAMESPACE;
            LOG.info("No cloudwatch namespace was specified, defaulting to '" + DEFAULT_NAMESPACE + "'.");
        }

        CloudWatchReporter.Builder builder =
                CloudWatchReporter.forRegistry(metricsRegistry, cloudWatchBuilder.build(), namespace);

        TimeUnit durationUnit = MetricsUtils.getMetricsDurationUnit(reporterConf);
        if (durationUnit != null) {
            builder.convertDurationsTo(durationUnit);
        }

        TimeUnit rateUnit = MetricsUtils.getMetricsRateUnit(reporterConf);
        if (rateUnit != null) {
            builder.convertRatesTo(rateUnit);
        }

        StormMetricsFilter filter = getMetricsFilter(reporterConf);
        if(filter != null){
            builder.filter(filter);
        }

        //defaults to 10
        reportingPeriod = getReportPeriod(reporterConf);

        //defaults to seconds
        reportingPeriodUnit = getReportPeriodUnit(reporterConf);

        reporter = builder.build();
    }

    private static Regions getCloudwatchRegion(Map reporterConf) {
        String regionName = Utils.getString(reporterConf.get(CLOUDWATCH_REGION), null);
        try {
            return Regions.fromName(regionName);
        } catch(IllegalArgumentException ex) {
            return null;
        }
    }

    private static String getCloudwatchNamespace(Map reporterConf) {
        return Utils.getString(reporterConf.get(CLOUDWATCH_NAMESPACE), null);
    }
}
