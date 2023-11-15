import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib'; 
import { Construct } from 'constructs';
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Code, Function, Runtime  } from 'aws-cdk-lib/aws-lambda';
import { RetentionDays } from 'aws-cdk-lib/aws-logs';
import { DefinitionBody, JsonPath, Map, Pass, StateMachine, Wait, WaitTime } from 'aws-cdk-lib/aws-stepfunctions';
import { DynamoAttributeValue, DynamoGetItem, LambdaInvoke } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as Config from "../config.json"


export class StpfncStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Create a DynamoDB table with a Partition Key
    // Note that the name of the partition key is needed 
    //  when setting up the Dynamo DB Get Item Step Function Task
    // Additionally you need the value for the key that Step Function DDB Get Item task will retrieve
    const ddbTable = new Table(this, 'DDB', {
      tableName: Config.ddb.tableName,
      partitionKey: { 
            name: Config.ddb.partitionKeyName, 
            type: AttributeType.STRING   
      },
      removalPolicy: RemovalPolicy.DESTROY
    })

    // Note the entry in `key` field in `DynamoGetItem`
    // is the name of the partition key
    // Copy it from the config.json file
    const readDDBTable = new DynamoGetItem(this, 'ReadDDBTable', {
      key: { mapstatelist : DynamoAttributeValue.fromString(Config.ddb.partitionKeyValueField) },
      table: ddbTable,
      outputPath: JsonPath.stringAt('$.Item.values.L')
    })

    const lambdaFunction = new Function(this, 'Lambda', {
      code: Code.fromAsset('src'),
      handler: 'handler.handler',
      runtime: Runtime.PYTHON_3_9,
      logRetention: RetentionDays.ONE_DAY,
    })

    const lambdaInvoke = new LambdaInvoke(this, 'LambdaInvoke', {
      lambdaFunction: lambdaFunction
    })

    const passState = new Pass(this, 'PassState')
    const passStateMap = new Pass(this, 'MapPassState', {
      // inputPath: "$.S"
    })

    const mapState = new Map(this, 'MapState', {
      maxConcurrency: Config.stepfunc.maxConcurrency
    })

    const waitState = new Wait(this, 'WaitInMap',{
      time: WaitTime.duration(Duration.seconds(Config.stepfunc.waitDurationSeconds))
    })

    const mapStateDefinition = passStateMap
    .next(waitState)
    .next(lambdaInvoke)

    mapState.iterator(mapStateDefinition)

    const stateMachineDefinition = readDDBTable
    .next(mapState)

    const sfMachine = new StateMachine(this, 'TestStateMachine', {
      definitionBody: DefinitionBody.fromChainable(stateMachineDefinition),
      timeout: Duration.seconds(Config.stepfunc.timeoutDurationSeconds),
      comment: 'Step Functions using CDK'
    })

    new CfnOutput(this, 'StateMachineArn', {
      value: sfMachine.stateMachineArn
    })

    new CfnOutput(this, 'StateMachineName', {
      value: sfMachine.stateMachineName
    })




  }
}
