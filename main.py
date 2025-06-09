import kfp
from kfp import dsl

@dsl.pipeline(
    name='End-to-End Student Engagement Pipeline',
    description='A pipeline that fuses data, prepares it, and trains a model.'
)
def student_engagement_pipeline(
    base_dir: str = 'gs://project-abd', 
    project_id: str = 'project-big-data-461104' 
):
    
    fusion_op = kfp.components.load_component_from_file('components/data_fusion_component.yaml')
    prep_op = kfp.components.load_component_from_file('components/data_prep_component.yaml')
    train_op = kfp.components.load_component_from_file('components/model_training_component.yaml')

    participant_info_data = f'{base_dir}/raw/participant_class_info/'
    survey_info_data = f'{base_dir}/raw/survey/'
    class_table_data = f'{base_dir}/raw/participant_class_info/' 
    wearable_data = f'{base_dir}/raw/class_wearable_data/'

    fusion_task = fusion_op(
        participant_info=participant_info_data,
        survey_info=survey_info_data,
        class_table=class_table_data,
        wearable_data=wearable_data
    )
    fusion_task.set_memory_limit('6G').set_cpu_limit('1.5')

    prep_task = prep_op(
        input_path=fusion_task.outputs['fused_data_path']
    )
    prep_task.set_memory_limit('6G').set_cpu_limit('1.5')

    train_task = train_op(
        train_data=prep_task.outputs['train_output_path'],
        test_data=prep_task.outputs['test_output_path'],
    )
    train_task.set_memory_limit('4G').set_cpu_limit('1')

if __name__ == '__main__':
    kfp.compiler.Compiler().compile(
        pipeline_func=student_engagement_pipeline,
        package_path='student_engagement_pipeline.yaml'
    )
    print("Pipeline compiled successfully to student_engagement_pipeline.yaml")