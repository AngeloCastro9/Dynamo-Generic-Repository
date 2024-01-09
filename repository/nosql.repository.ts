import DynamoRepository from '../dynamo.repository';

export class NoSqlDynamoRepository extends DynamoRepository {
  constructor(readonly region: string) {
    super(region);
  }
}
