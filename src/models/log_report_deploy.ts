import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface LogReportDeployInterface extends Model {
    id: number;
    status: number;
    package_id: number;
    client_unique_id: string;
    previous_label: string;
    previous_deployment_key: string;
    created_at: Date;
}

export const LogReportDeploy = sequelize.define<LogReportDeployInterface>(
    'LogReportDeploy',
    {
        id: {
            type: DataTypes.BIGINT({ length: 20 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        status: DataTypes.INTEGER({ length: 3 }),
        package_id: DataTypes.INTEGER({ length: 10 }),
        client_unique_id: DataTypes.STRING,
        previous_label: DataTypes.STRING,
        previous_deployment_key: DataTypes.STRING,
        created_at: DataTypes.DATE,
    },
    {
        tableName: 'log_report_deploy',
        underscored: true,
        updatedAt: false,
        paranoid: true,
    },
);
