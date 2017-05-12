package com.swhy.processor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.util.TextLineDemarcator;

import com.swhy.processor.utils.ResultLogReader;

@Tags({ "result", "split", "log" })
@CapabilityDescription("解析result日志")
@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = ""),
		@ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class ResultLogProcessor extends AbstractProcessor {

	//attribute keys
	public static final String SPLIT_LINE_COUNT= "text.line.count";
	public static final String TEXT_CHARSET = "text.charset";
	
	// nifi processor的properties
	//
	public static final PropertyDescriptor INPUT_TEXT_CHARSET = new PropertyDescriptor.Builder().name("") // 属性名
			.displayName("Input Text Charset") // 部署名
			.description("文件编码格式") // 描述
			.required(true) // 属性是否为必须填写
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)// 校验字符编码
			.build();
	// processor的关系
	public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("原始的输入文件将被路由到这个目标，当它成功地分割成一个或多个文件时")
            .build();
    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("当一个输入文件成功地分割成一个或多个分割文件时，分割文件将被路由到这个目的地。")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("如果由于某种原因无法分割文件，那么原始文件将被路由到该目的地，而其他任何地方都不会被路由到别处")
            .build();

	private static List<PropertyDescriptor> descriptors; // 设置属性集合
	private static Set<Relationship> relationships; // 关系集合

	
	static{
		descriptors = Collections.unmodifiableList(Arrays.asList(new PropertyDescriptor[]{
				//TODO 属性待定
				INPUT_TEXT_CHARSET
		}));
		
		
		relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(new Relationship[]{
				REL_ORIGINAL,
                REL_SPLITS,
                REL_FAILURE
		})));
	}
	
	
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}
	
	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}
	
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile sourceFlowFile = session.get();
		if(sourceFlowFile == null){
			return;
		}
		
//		AtomicBoolean error = new AtomicBoolean();
		InputStream inputStream = session.read(sourceFlowFile);
		
	}

}
